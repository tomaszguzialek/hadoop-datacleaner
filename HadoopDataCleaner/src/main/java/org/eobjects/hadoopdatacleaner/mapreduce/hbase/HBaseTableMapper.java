package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.AnalyzerJob;
import org.eobjects.analyzer.job.runner.ConsumeRowHandler;
import org.eobjects.analyzer.util.LabelUtils;
import org.eobjects.hadoopdatacleaner.FlatFileTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.datastores.HBaseParser;
import org.eobjects.hadoopdatacleaner.datastores.hbase.utils.ResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableMapper extends TableMapper</* KEYOUT */Text, /* VALUEOUT */SortedMapWritable> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableMapper.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;

    private AnalysisJob analysisJob;

    private HBaseParser hbaseParser;

    @Override
    protected void setup(
            org.apache.hadoop.mapreduce.Mapper</* KEYIN */ImmutableBytesWritable, /* VALUEIN */Result, /* KEYOUT */Text, /* VALUEOUT */SortedMapWritable>.Context context)
            throws IOException, InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        analyzerBeansConfiguration = ConfigurationSerializer
                .deserializeAnalyzerBeansDatastores(datastoresConfigurationLines);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);
        hbaseParser = new HBaseParser(analysisJob.getSourceColumns());
        super.setup(context);
    }

    public void map(/* KEYIN */ImmutableBytesWritable row, /* VALUEIN */Result result, Context context)
            throws InterruptedException, IOException {

        InputRow inputRow = hbaseParser.prepareRow(result);

        ConsumeRowHandler.Configuration configuration = new ConsumeRowHandler.Configuration();
        configuration.includeAnalyzers = false;
        ConsumeRowHandler consumeRowHandler = new ConsumeRowHandler(analysisJob, analyzerBeansConfiguration,
                configuration);
        List<InputRow> transformedRows = consumeRowHandler.consume(inputRow);

        for (InputRow transformedRow : transformedRows) {
            SortedMapWritable rowWritable = new SortedMapWritable();
            for (InputColumn<?> inputColumn : transformedRow.getInputColumns()) {
                String columnName = inputColumn.getName();
                Object value = transformedRow.getValue(inputColumn);
                rowWritable.put(new Text(columnName), new Text(value.toString()));
            }
            for (AnalyzerJob analyzerJob : analysisJob.getAnalyzerJobs()) {
                context.write(new Text(LabelUtils.getLabel(analyzerJob)), rowWritable);
            }
        }

        ResultUtils.printResult(result, logger);
    }
}