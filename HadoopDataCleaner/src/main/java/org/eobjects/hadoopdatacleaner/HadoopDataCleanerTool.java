package org.eobjects.hadoopdatacleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;

public class HadoopDataCleanerTool extends Configured {

    public static final String ANALYSIS_JOB_XML_KEY = "analysis.job.xml";
    public static final String ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY = "analyzer.beans.configuration.datastores.key";
    
    protected String analyzerBeansConfigurationDatastores;
    protected String analysisJobXml;

    public HadoopDataCleanerTool(AnalyzerBeansConfiguration analyzerBeansConfiguration, AnalysisJob analysisJob) {

        this.analyzerBeansConfigurationDatastores = ConfigurationSerializer
                .serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
        this.analysisJobXml = ConfigurationSerializer
                .serializeAnalysisJobToXml(analyzerBeansConfiguration, analysisJob);
    }

    public HadoopDataCleanerTool(Configuration conf) {
        super(conf);
    }

}