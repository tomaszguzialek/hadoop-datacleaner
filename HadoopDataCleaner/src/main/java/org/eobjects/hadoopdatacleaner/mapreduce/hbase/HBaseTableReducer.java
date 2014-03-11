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
package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzer;
import org.eobjects.analyzer.beans.valuedist.ValueDistributionAnalyzerResult;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.job.AnalyzerJob;
import org.eobjects.analyzer.lifecycle.LifeCycleHelper;
import org.eobjects.analyzer.util.LabelUtils;
import org.eobjects.hadoopdatacleaner.FlatFileTool;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.datastores.RowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableReducer extends
        TableReducer</* KEYIN */Text, /* VALUEIN */SortedMapWritable, /* KEYOUT */NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableReducer.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;
    
    private AnalysisJob analysisJob;

    @Override
    protected void setup(
            org.apache.hadoop.mapreduce.Reducer</* KEYIN */Text, /* VALUEIN */SortedMapWritable, /* KEYOUT */NullWritable, /*VALUEOUT*/ Writable>.Context context)
            throws IOException, InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        analyzerBeansConfiguration = ConfigurationSerializer
                .deserializeAnalyzerBeansDatastores(datastoresConfigurationLines);
        analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);
        super.setup(context);
    }
    
    public void reduce(Text analyzerKey, Iterable<SortedMapWritable> writableResults, Context context)
            throws IOException, InterruptedException {

        ValueDistributionAnalyzer analyzer = initializeAnalyzer(analyzerKey);
        
        logger.info("analyzerKey = " + analyzerKey.toString() + " rows: ");
        for (SortedMapWritable rowWritable : writableResults) {
            InputRow inputRow = RowUtils.sortedMapWritableToInputRow(rowWritable, analysisJob.getSourceColumns());
            analyzer.run(inputRow, 1);
            
            Result result = RowUtils.sortedMapWritableToResult(rowWritable);
            RowUtils.printResult(result, logger);
            Put put = RowUtils.preparePut(result);
            context.write(NullWritable.get(), put);
        }
        logger.info("end of analyzerKey = " + analyzerKey.toString() + " rows.");
        
        ValueDistributionAnalyzerResult analyzerResult = analyzer.getResult();
        logger.debug("analyzerResult.getName(): " + analyzerResult.getName());
        logger.debug("analyzerResult.toString(): " + analyzerResult.toString());
    }

    private ValueDistributionAnalyzer initializeAnalyzer(Text analyzerKey) {
        
        ValueDistributionAnalyzer analyzer = new ValueDistributionAnalyzer();
        AnalyzerJob analyzerJob = null;
        
        for (AnalyzerJob analyzerJobIter : analysisJob.getAnalyzerJobs()) {
            if (LabelUtils.getLabel(analyzerJobIter).equals(analyzerKey.toString())) {
                analyzerJob = analyzerJobIter;
                break;
            }
        }
        
        LifeCycleHelper lifeCycleHelper = new LifeCycleHelper(analyzerBeansConfiguration.getInjectionManager(analysisJob), true);
        lifeCycleHelper.assignConfiguredProperties(analyzerJob.getDescriptor(), analyzer, analyzerJob.getConfiguration());
        lifeCycleHelper.assignProvidedProperties(analyzerJob.getDescriptor(), analyzer);
        
        return analyzer;
    }
}
