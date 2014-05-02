/**
 * DataCleaner (community edition)
 * Copyright (C) 2013 Human Inference

 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.hadoopdatacleaner.tools;

import java.io.IOException;

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
    
    public HadoopDataCleanerTool(AnalyzerBeansConfiguration analyzerBeansConfiguration, String analysisJobXml) throws IOException {

        this.analyzerBeansConfigurationDatastores = ConfigurationSerializer
                .serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
        this.analysisJobXml = analysisJobXml;
    }

    public HadoopDataCleanerTool(Configuration conf) {
        super(conf);
    }

}