package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
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
import org.eobjects.analyzer.job.runner.RowIdGenerator;
import org.eobjects.analyzer.job.runner.RowProcessingConsumer;
import org.eobjects.analyzer.job.runner.RowProcessingPublisher;
import org.eobjects.analyzer.job.runner.RowProcessingPublishers;
import org.eobjects.analyzer.job.tasks.ConsumeRowTask;
import org.eobjects.analyzer.job.tasks.Task;
import org.eobjects.analyzer.lifecycle.LifeCycleHelper;
import org.eobjects.analyzer.result.AnalyzerResult;
import org.eobjects.analyzer.util.ReflectionUtils;
import org.eobjects.analyzer.util.SourceColumnFinder;
import org.eobjects.hadoopdatacleaner.HadoopDataCleanerTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.mapreduce.writables.TextArrayWritable;

public class HadoopDataCleanerMapper extends
		Mapper<LongWritable, Text, LongWritable, TextArrayWritable> {

	private AnalyzerBeansConfiguration analyzerBeansConfiguration;

	private AnalysisJob analysisJob;

	private TaskRunner taskRunner = new SingleThreadedTaskRunner();

	private AnalysisListener analysisListener = prepareAnalysisListener();

	private final RowIdGenerator idGenerator = prepareIdGenerator();

	private Collection<InputColumn<?>> sourceColumns;
	
	private LongWritable unit = new LongWritable(1);

	@Override
	public void map(LongWritable key, Text csvLine, Context context)
			throws IOException, InterruptedException {

		Configuration mapReduceConfiguration = context.getConfiguration();
		String datastoresCsvLines = mapReduceConfiguration
				.get(HadoopDataCleanerTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_CSV_KEY);
		String analysisJobXml = mapReduceConfiguration
				.get(HadoopDataCleanerTool.ANALYSIS_JOB_XML_KEY);
		analyzerBeansConfiguration = ConfigurationSerializer.deserializeDatastoresFromCsv(datastoresCsvLines);
		analysisJob = removeAnalyzers(ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration));

		InputRow inputRow = prepareRow(csvLine);
		

		RowProcessingPublisher publisher = prepareRowProcessingPublisher();
		List<RowProcessingConsumer> consumers = prepareConsumers(publisher);

		executeConsumeRowTask(inputRow, publisher, consumers);

		TextArrayWritable resultArray = collectResults(consumers);

		// clean up
		publisher.closeConsumers();
		
		context.write(unit, resultArray);
	}

	private AnalysisJob removeAnalyzers(AnalysisJob analysisJob) {
		// TODO: Removing analyzers
//		Collection<AnalyzerJob> analyzerJobs = analysisJob.getAnalyzerJobs();
//		for (AnalyzerJob analyzerJob : analyzerJobs) {
//			analyzerJobs.remove(analyzerJob);
//		}
		return analysisJob;
	}

	private TextArrayWritable collectResults(
			List<RowProcessingConsumer> consumers) {
		List<AnalyzerResult> results = new ArrayList<AnalyzerResult>();
		for (RowProcessingConsumer consumer : consumers) {
			Object component = consumer.getComponent();
			if (component instanceof Analyzer<?>) {
				AnalyzerResult result = ((Analyzer<?>) component).getResult();
				results.add(result);
			}
		}
		
		TextArrayWritable resultArray = new TextArrayWritable();
		Writable[] writableArray = new Writable[results.size()];
		int i = 0;
		for (AnalyzerResult analyzerResult : results) {
			writableArray[i++] = new Text(analyzerResult.toString());
			System.out.println("Result: \n" + analyzerResult.toString());
		}
		resultArray.set(writableArray);
		return resultArray;
	}

	private List<RowProcessingConsumer> prepareConsumers(
			RowProcessingPublisher publisher) {
		publisher.initializeConsumers(new TaskListener() {
			@Override
			public void onError(Task task, Throwable throwable) {
				System.out.println("Error");
				throwable.printStackTrace();
				System.exit(-1);
			}

			@Override
			public void onComplete(Task task) {
				System.out.println("Completed initialization of consumers");
			}

			@Override
			public void onBegin(Task task) {
				System.out.println("Beginning");
			}
		});

		List<RowProcessingConsumer> consumers = publisher
				.getConfigurableConsumers();

		consumers = RowProcessingPublisher.sortConsumers(consumers);
		for (RowProcessingConsumer consumer : consumers) {
			
			// TODO: Crazy hack, should be improved in AnalyzerBeans
			Method method = ReflectionUtils.getMethod(consumer.getClass(),
					"setRowIdGenerator", true);
			if (method != null) {
				method.setAccessible(true);
				try {
					method.invoke(consumer, idGenerator);
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(-1);
				}
			}
		}
		return consumers;
	}

	private void executeConsumeRowTask(InputRow row,
			RowProcessingPublisher publisher,
			List<RowProcessingConsumer> consumers) {

			OutcomeSink outcomes = new OutcomeSinkImpl();
			ConsumeRowTask task = new ConsumeRowTask(consumers, 0,
					publisher.getRowProcessingMetrics(), row, analysisListener,
					outcomes);

			task.execute();
	}

	private InputRow prepareRow(Text csvLine) {
		String[] values = csvLine.toString().split(";");

		sourceColumns = analysisJob.getSourceColumns();

		assert sourceColumns.size() == values.length;
		
		MockInputRow row = new MockInputRow();
		for (String value : values) {
			InputColumn<?> inputColumn = sourceColumns.iterator().next();
			row.put(inputColumn, value);
		}
		return row;
	}

	private RowProcessingPublisher prepareRowProcessingPublisher() {
		InjectionManager injectionManager = analyzerBeansConfiguration
				.getInjectionManager(analysisJob);
		ReferenceDataActivationManager referenceDataActivationManager = new ReferenceDataActivationManager();
		boolean includeNonDistributedTasks = true; // TODO: reconsider
		LifeCycleHelper lifeCycleHelper = new LifeCycleHelper(injectionManager,
				referenceDataActivationManager, includeNonDistributedTasks);
		SourceColumnFinder sourceColumnFinder = new SourceColumnFinder();
		sourceColumnFinder.addSources(analysisJob);

		RowProcessingPublishers rowProcessingPublishers = new RowProcessingPublishers(
				analysisJob, analysisListener, taskRunner, lifeCycleHelper,
				sourceColumnFinder);

		Collection<RowProcessingPublisher> publisherCollection = rowProcessingPublishers
				.getRowProcessingPublishers();

		assert publisherCollection.size() == 1;

		RowProcessingPublisher publisher = publisherCollection.iterator()
				.next();
		return publisher;
	}

	private RowIdGenerator prepareIdGenerator() {
		return new RowIdGenerator() {
			int nextVirtual;
			int nextPhysical;

			@Override
			public int nextVirtualRowId() {
				nextVirtual++;
				return nextVirtual;
			}

			@Override
			public int nextPhysicalRowId() {
				nextPhysical++;
				return nextPhysical;
			}
		};
	}

	private AnalysisListener prepareAnalysisListener() {
		return new InfoLoggingAnalysisListener() {
			@Override
			public void errorInTransformer(AnalysisJob job,
					TransformerJob transformerJob, InputRow row,
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
