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
package org.eobjects.hadoopdatacleaner.mapreduce;

import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;

public class MapperReducerDelegate {

    protected AnalyzerBeansConfiguration analyzerBeansConfiguration;

    protected AnalysisJob analysisJob;
    
    /**
     * 
     */
    public MapperReducerDelegate(AnalyzerBeansConfiguration analyzerBeansConfiguration, AnalysisJob analysisJob) {
        this.analyzerBeansConfiguration = analyzerBeansConfiguration;
        this.analysisJob = analysisJob;
    }
    
    public MapperReducerDelegate(String analyzerBeansConfigurationDatastores, String analysisJobXml) {
        analyzerBeansConfiguration = ConfigurationSerializer
                .deserializeAnalyzerBeansDatastores(analyzerBeansConfigurationDatastores);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);
    }
    
    /**
     * @return the analysisJob
     */
    public AnalysisJob getAnalysisJob() {
        return analysisJob;
    }
    
}