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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.runner.ConsumeRowHandler;
import org.eobjects.analyzer.job.runner.ConsumeRowResult;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperDelegate extends MapperReducerDelegate {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(MapperDelegate.class);

    /**
     * 
     */
    public MapperDelegate(String analyzerBeansConfigurationDatastores, String analysisJobXml) {
        super(analyzerBeansConfigurationDatastores, analysisJobXml);
    }
    
    public MapperDelegate(AnalyzerBeansConfiguration analyzerBeansConfiguration, AnalysisJob analysisJob, Callback mapperEmitterCallback) {
        super(analyzerBeansConfiguration, analysisJob);
    }
    
    public void run(InputRow inputRow, Callback mapperEmitterCallback) throws IOException, InterruptedException {
        ConsumeRowHandler.Configuration configuration = new ConsumeRowHandler.Configuration();
        configuration.includeAnalyzers = false;
        ConsumeRowHandler consumeRowHandler = new ConsumeRowHandler(analysisJob, analyzerBeansConfiguration,
                configuration);
        ConsumeRowResult consumeRowResult = consumeRowHandler.consumeRow(inputRow);
        
        List<InputRow> inputRows = new ArrayList<InputRow>();
        inputRows.add(inputRow);
        ConsumeRowResult noOperationResult = new ConsumeRowResult(inputRows, consumeRowResult.getOutcomeSinks());
        
        MapperEmitter mapperEmitter = new MapperEmitter(mapperEmitterCallback);
        mapperEmitter.emit(noOperationResult, analysisJob.getAnalyzerJobs());
    }
}
