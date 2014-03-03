package org.eobjects.hadoopdatacleaner.mapreduce.flatfile;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.runner.ConsumeRowHandler;
import org.eobjects.hadoopdatacleaner.FlatFileTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.datastores.CsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatFileMapper extends Mapper<LongWritable, Text, LongWritable, SortedMapWritable> {

    private static final Logger logger = LoggerFactory.getLogger(FlatFileMapper.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;

    private AnalysisJob analysisJob;

    private CsvParser csvParser = new CsvParser();

    protected void setup(Mapper<LongWritable, Text, LongWritable, SortedMapWritable>.Context context)
            throws IOException, InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        analyzerBeansConfiguration = ConfigurationSerializer.deserializeAnalyzerBeansDatastores(datastoresConfigurationLines);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);
    }

    @Override
    public void map(LongWritable key, Text csvLine, Context context) throws IOException, InterruptedException {
        csvParser.parseHeaderRow(csvLine, analysisJob);
        InputRow inputRow = csvParser.prepareRow(csvLine);

        ConsumeRowHandler.Configuration configuration = new ConsumeRowHandler.Configuration();
        configuration.includeAnalyzers = false;
        ConsumeRowHandler consumeRowHandler = new ConsumeRowHandler(analysisJob, analyzerBeansConfiguration, configuration);
        List<InputRow> transformedRows = consumeRowHandler.consume(inputRow);
 
        for (InputRow transformedRow : transformedRows) {
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
                context.write(key, rowWritable);
            }
        }
    }
}
