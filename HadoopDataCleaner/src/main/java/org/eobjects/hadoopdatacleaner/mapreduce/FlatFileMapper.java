package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eobjects.analyzer.beans.api.Analyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.InjectionManager;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.TransformerJob;
import org.eobjects.analyzer.job.concurrent.SingleThreadedTaskRunner;
import org.eobjects.analyzer.job.concurrent.TaskListener;
import org.eobjects.analyzer.job.runner.AnalysisJobMetrics;
import org.eobjects.analyzer.job.runner.AnalysisListener;
import org.eobjects.analyzer.job.runner.InfoLoggingAnalysisListener;
import org.eobjects.analyzer.job.runner.OutcomeSink;
import org.eobjects.analyzer.job.runner.OutcomeSinkImpl;
import org.eobjects.analyzer.job.runner.ReferenceDataActivationManager;
import org.eobjects.analyzer.job.runner.RowProcessingConsumer;
import org.eobjects.analyzer.job.runner.RowProcessingPublisher;
import org.eobjects.analyzer.job.runner.RowProcessingPublishers;
import org.eobjects.analyzer.job.tasks.Task;
import org.eobjects.analyzer.lifecycle.LifeCycleHelper;
import org.eobjects.analyzer.util.SourceColumnFinder;
import org.eobjects.hadoopdatacleaner.FlatFileTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.datastores.CsvParser;
import org.eobjects.hadoopdatacleaner.job.tasks.ConsumeRowTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatFileMapper extends Mapper<LongWritable, Text, LongWritable, SortedMapWritable> {

    private static final Logger logger = LoggerFactory.getLogger(FlatFileMapper.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;

    private AnalysisJob analysisJob;

    private AnalysisListener analysisListener = prepareAnalysisListener();
    
    private CsvParser csvParser = new CsvParser();

    @Override
    public void map(LongWritable key, Text csvLine, Context context) throws IOException, InterruptedException {

        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresCsvLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_CSV_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        analyzerBeansConfiguration = ConfigurationSerializer.deserializeDatastoresFromCsv(datastoresCsvLines);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);

        csvParser.parseHeaderRow(csvLine, analysisJob);
        InputRow inputRow = csvParser.prepareRow(csvLine);
        
        

        RowProcessingPublisher publisher = prepareRowProcessingPublisher();
        List<RowProcessingConsumer> consumers = prepareConsumers(publisher);

        InputRow transformedRow = executeConsumeRowTask(inputRow, publisher, consumers);

        logger.info("Transformed row: ");
        for (InputColumn<?> inputColumn : transformedRow.getInputColumns()) {
            Object value = transformedRow.getValue(inputColumn);
            logger.info("\t" + inputColumn.getName() + ": " + value);
        }
        
        SortedMapWritable rowWritable = new SortedMapWritable();
        for (InputColumn<?> inputColumn : transformedRow.getInputColumns()) {
            String columnName = inputColumn.getName();
            Object value = transformedRow.getValue(inputColumn);
            System.out.println(columnName);
            if (value == null)
                System.out.println(columnName);
            System.out.println(value.toString());
            rowWritable.put(new Text(columnName), new Text(value.toString()));
        }

        // clean up
        publisher.closeConsumers();

         context.write(key, rowWritable);
    }

    private List<RowProcessingConsumer> prepareConsumers(RowProcessingPublisher publisher) {
        publisher.initializeConsumers(new TaskListener() {
            @Override
            public void onError(Task task, Throwable throwable) {
                logger.error("Exception thrown while initializing consumers.", throwable);
                System.exit(-1);
            }

            @Override
            public void onComplete(Task task) {
                logger.info("Consumers initialized successfully.");
            }

            @Override
            public void onBegin(Task task) {
                logger.info("Beginning the process of initializing consumers.");
            }
        });

        List<RowProcessingConsumer> consumersWithoutAnalyzers = removeAnalyzers(publisher.getConfigurableConsumers());

        consumersWithoutAnalyzers = RowProcessingPublisher.sortConsumers(consumersWithoutAnalyzers);

        return consumersWithoutAnalyzers;
    }

    private List<RowProcessingConsumer> removeAnalyzers(List<RowProcessingConsumer> configurableConsumers) {
        List<RowProcessingConsumer> consumersWithoutAnalyzers = new ArrayList<RowProcessingConsumer>();
        for (RowProcessingConsumer rowProcessingConsumer : configurableConsumers) {
            Object component = rowProcessingConsumer.getComponent();
            if (!(component instanceof Analyzer<?>)) {
                consumersWithoutAnalyzers.add(rowProcessingConsumer);
            }
        }
        return consumersWithoutAnalyzers;
    }

    private InputRow executeConsumeRowTask(InputRow row, RowProcessingPublisher publisher,
            List<RowProcessingConsumer> consumers) {

        OutcomeSink outcomes = new OutcomeSinkImpl();
        ConsumeRowTask task = new ConsumeRowTask(consumers, 0, publisher.getRowProcessingMetrics(), row,
                analysisListener, outcomes);
        
        task.execute();
        return task.getCurrentRow();
    }

    

    private RowProcessingPublisher prepareRowProcessingPublisher() {
        InjectionManager injectionManager = analyzerBeansConfiguration.getInjectionManager(analysisJob);
        ReferenceDataActivationManager referenceDataActivationManager = new ReferenceDataActivationManager();
        boolean includeNonDistributedTasks = true; // TODO: reconsider
        LifeCycleHelper lifeCycleHelper = new LifeCycleHelper(injectionManager, referenceDataActivationManager,
                includeNonDistributedTasks);
        SourceColumnFinder sourceColumnFinder = new SourceColumnFinder();
        sourceColumnFinder.addSources(analysisJob);

        RowProcessingPublishers rowProcessingPublishers = new RowProcessingPublishers(analysisJob, analysisListener,
                new SingleThreadedTaskRunner(), lifeCycleHelper, sourceColumnFinder);

        Collection<RowProcessingPublisher> publisherCollection = rowProcessingPublishers.getRowProcessingPublishers();

        assert publisherCollection.size() == 1;

        RowProcessingPublisher publisher = publisherCollection.iterator().next();
        return publisher;
    }

    private AnalysisListener prepareAnalysisListener() {
        return new InfoLoggingAnalysisListener() {
            
            @Override
            public void errorInTransformer(AnalysisJob job, TransformerJob transformerJob, InputRow row,
                    Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void errorUknown(AnalysisJob job, Throwable throwable) {
                throwable.printStackTrace();
            }
             
            @Override
            public void jobSuccess(AnalysisJob job, AnalysisJobMetrics metrics) {
                logger.info("Job finished successfully.");
                // FIXME: Not appearing anywhere...
            }
        };

    }

}
