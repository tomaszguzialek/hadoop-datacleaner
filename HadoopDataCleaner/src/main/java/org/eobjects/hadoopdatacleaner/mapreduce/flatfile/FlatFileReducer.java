package org.eobjects.hadoopdatacleaner.mapreduce.flatfile;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.FlatFileTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatFileReducer extends Reducer<LongWritable, SortedMapWritable, LongWritable, Text> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(FlatFileReducer.class);
    
    private AnalyzerBeansConfiguration analyzerBeansConfiguration;

    private AnalysisJob analysisJob;
    
    protected void setup(Reducer<LongWritable, SortedMapWritable, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        analyzerBeansConfiguration = ConfigurationSerializer.deserializeAnalyzerBeansDatastores(datastoresConfigurationLines);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);
        super.setup(context);
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<SortedMapWritable> rows, Context context) throws IOException,
            InterruptedException {
        
        Text finalText = new Text();
        for (SortedMapWritable row : rows) {
            for (@SuppressWarnings("rawtypes")
            Iterator<Entry<WritableComparable, Writable>> iterator = row.entrySet().iterator(); iterator.hasNext();) {
                Text value = ((Text) iterator.next().getValue());
                finalText.set(finalText.toString() + value.toString());
                if (iterator.hasNext())
                    finalText.set(finalText.toString() + ";");
                else
                    finalText.set(finalText.toString() + "\n");
            }
        }
        context.write(key, finalText);
    }

}
