/**
 * eobjects.org AnalyzerBeans
 * Copyright (C) 2010 eobjects.org
 *
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
package org.eobjects.hadoopdatacleaner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.sample.SampleHBaseConfiguration;
import org.eobjects.hadoopdatacleaner.mapreduce.hbase.HBaseTableMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.hbase.HBaseTableReducer;

public final class HBaseTool extends HadoopDataCleanerTool implements Tool {

    public HBaseTool(AnalyzerBeansConfiguration analyzerBeansConfiguration, AnalysisJob analysisJob) {
        super(analyzerBeansConfiguration, analysisJob);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputTableName, outputTableName;
        if (args.length == 2) {
            inputTableName = args[0];
            outputTableName = args[1];
        } else {
            System.err.println("Incorrect number of arguments.  Expected: <inputTableName> <outputTableName>");
            return -1;
        }

        Configuration mapReduceConfiguration = HBaseConfiguration.create();
        mapReduceConfiguration.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY, analyzerBeansConfigurationDatastores);
        mapReduceConfiguration.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);

        return runMapReduceJob(inputTableName, outputTableName, mapReduceConfiguration);
    }

    private int runMapReduceJob(String inputTableName, String outputTableName, Configuration mapReduceConfiguration)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(mapReduceConfiguration);
        job.setJarByClass(HBaseTableMapper.class);
        job.setJobName(this.getClass().getName());

        Scan scan = new Scan();
        scan.setCaching(500); // 1 is the default in Scan, which will be bad for
                              // MapReduce jobs
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        TableMapReduceUtil.initTableMapperJob(inputTableName, // input HBase
                                                              // table name
                scan, // Scan instance to control CF and attribute selection
                HBaseTableMapper.class, // mapper
                Text.class, // mapper output key
                SortedMapWritable.class, // mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(outputTableName, // output HBase
                                                                // table name
                HBaseTableReducer.class, // reducer class
                job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        AnalyzerBeansConfiguration analyzerBeansConfiguration = SampleHBaseConfiguration.buildAnalyzerBeansConfiguration();
        AnalysisJob analysisJob = SampleHBaseConfiguration.buildAnalysisJob(analyzerBeansConfiguration);
        HBaseTool hBaseTool = new HBaseTool(analyzerBeansConfiguration, analysisJob);
        ToolRunner.run(hBaseTool, args);
    }
    
}
