package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.eobjects.analyzer.beans.api.Analyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.configuration.InjectionManager;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MetaModelInputColumn;
import org.eobjects.analyzer.data.MockInputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.TransformerJob;
import org.eobjects.analyzer.job.concurrent.SingleThreadedTaskRunner;
import org.eobjects.analyzer.job.concurrent.TaskListener;
import org.eobjects.analyzer.job.concurrent.TaskRunner;
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
import org.eobjects.hadoopdatacleaner.HadoopDataCleanerTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.job.tasks.ConsumeRowTask;
import org.eobjects.hadoopdatacleaner.mapreduce.writables.TextArrayWritable;
import org.eobjects.metamodel.schema.ImmutableColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopDataCleanerMapper extends Mapper<LongWritable, Text, LongWritable, TextArrayWritable> {

    private static final Logger logger = LoggerFactory.getLogger(HadoopDataCleanerMapper.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;

    private AnalysisJob analysisJob;

    private TaskRunner taskRunner = new SingleThreadedTaskRunner();

    private AnalysisListener analysisListener = prepareAnalysisListener();

    private Collection<InputColumn<?>> jobColumns;

    private Collection<Boolean> usedColumns;

    private LongWritable unit = new LongWritable(1);

    @Override
    public void map(LongWritable key, Text csvLine, Context context) throws IOException, InterruptedException {

        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresCsvLines = mapReduceConfiguration
                .get(HadoopDataCleanerTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_CSV_KEY);
        String analysisJobXml = mapReduceConfiguration.get(HadoopDataCleanerTool.ANALYSIS_JOB_XML_KEY);
        analyzerBeansConfiguration = ConfigurationSerializer.deserializeDatastoresFromCsv(datastoresCsvLines);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);

        parseHeaderRow(csvLine);
        InputRow inputRow = prepareRow(csvLine);

        RowProcessingPublisher publisher = prepareRowProcessingPublisher();
        List<RowProcessingConsumer> consumers = prepareConsumers(publisher);

        InputRow transformedRow = executeConsumeRowTask(inputRow, publisher, consumers);

        logger.info("Transformed row: ");
        for (InputColumn<?> inputColumn : transformedRow.getInputColumns()) {
            Object value = transformedRow.getValue(inputColumn);
            logger.info("\t" + inputColumn.getName() + ": " + value);
        }

        // TextArrayWritable result =
        // transformRowToTextArrayWritable(transformedRow);

        // clean up
        publisher.closeConsumers();

        // context.write(unit, new Text);
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
        return task.getRow();
    }

    // private TextArrayWritable transformRowToTextArrayWritable(InputRow row) {
    // TextArrayWritable resultArray = new TextArrayWritable();
    // Writable[] writableArray = new Writable[results.size()];
    // int i = 0;
    // for (OutputColumns outputColumns : results) {
    // writableArray[i++] = new Text(outputColumns.toString());
    // System.out.println("Result: \n" + outputColumns.toString());
    // }
    // resultArray.set(writableArray);
    // return resultArray;
    // }

    private void parseHeaderRow(Text csvLine) {
        if (usedColumns == null) {
            usedColumns = new ArrayList<Boolean>();
            jobColumns = analysisJob.getSourceColumns();

            String[] values = csvLine.toString().split(";");


            for (String value : values) {
                Boolean found = false;
                for (Iterator<InputColumn<?>> jobColumnsIterator = jobColumns.iterator(); jobColumnsIterator.hasNext();) {
                    InputColumn<?> jobColumn = (InputColumn<?>) jobColumnsIterator.next();
                    String shortName = jobColumn.getName().substring(jobColumn.getName().lastIndexOf('.') + 1);
                    if (shortName.equals(value)) {
                        found = true;
                        break;
                    }
                }
                usedColumns.add(found);
            }
        }

    }

    private InputRow prepareRow(Text csvLine) {
        String[] values = csvLine.toString().split(";");

        Iterator<InputColumn<?>> jobColumnsIterator = jobColumns.iterator();
        Iterator<Boolean> usedColumnsIterator = usedColumns.iterator();
        
        int usedColumnsCount = 0;
        for (Boolean used : usedColumns) {
            if (used == true)
                usedColumnsCount++;
        }

//        if (values.length != usedColumnsCount) {
//            throw new IllegalStateException(
//                    "The number of values in the row does not match the numbers of columns declared. Values: "
//                            + values.length + ", columns declared: " + jobColumns.size());
//        }

        MockInputRow row = new MockInputRow();
        for (String value : values) {
            Boolean used = usedColumnsIterator.next();
            if (used) {
                InputColumn<?> inputColumn = jobColumnsIterator.next();
                row.put(inputColumn, value);
            }
        }
        return row;
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
                taskRunner, lifeCycleHelper, sourceColumnFinder);

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
        };

    }

}
