package org.eobjects.hadoopdatacleaner;

import junit.framework.Assert;

import org.apache.hadoop.util.ToolRunner;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.sample.SampleCsvConfiguration;
import org.junit.Test;

public class FlatFileToolTest {

    FlatFileTool flatFileTool;

    @Test
    public void test() throws Exception {
        String[] args = new String[2];
        args[0] = "/home/cloudera/datacleaner_examples/countrycodes.csv";
        args[1] = "output";
        AnalyzerBeansConfiguration analyzerBeansConfiguration = SampleCsvConfiguration
                .buildAnalyzerBeansConfigurationLocal(args[0]);
        AnalysisJob analysisJob = SampleCsvConfiguration.buildAnalysisJob(analyzerBeansConfiguration);
        flatFileTool = new FlatFileTool(analyzerBeansConfiguration, analysisJob);
        int exitCode = ToolRunner.run(flatFileTool, args);
        Assert.assertEquals(0, exitCode);
    }

}
