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
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.sample.SampleCassandraConfiguration;
import org.eobjects.hadoopdatacleaner.mapreduce.cassandra.CassandraMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.cassandra.CassandraReducer;

public final class CassandraTool extends HadoopDataCleanerTool implements Tool {

    public CassandraTool(AnalyzerBeansConfiguration analyzerBeansConfiguration, AnalysisJob analysisJob) {
        super(analyzerBeansConfiguration, analysisJob);
    }

    @Override
    public int run(String[] args) throws Exception {
        String inputColumnFamilyName, outputColumnFamilyName;
        if (args.length == 2) {
            inputColumnFamilyName = args[0];
            outputColumnFamilyName = args[1];
        } else {
            System.err.println("Incorrect number of arguments.  Expected: <inputColumnFamilyName> <outputColumnFamilyName>");
            return -1;
        }

        Configuration mapReduceConfiguration = getConf();
        mapReduceConfiguration.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY, analyzerBeansConfigurationDatastores);
        mapReduceConfiguration.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);

        return runMapReduceJob(inputColumnFamilyName, outputColumnFamilyName, mapReduceConfiguration);
    }

    private int runMapReduceJob(String inputColumnFamilyName, String outputColumnFamilyName, Configuration mapReduceConfiguration)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job job = Job.getInstance(mapReduceConfiguration);
        job.setJarByClass(CassandraTool.class);
        job.setJobName(this.getClass().getName());

        job.setMapperClass(CassandraMapper.class);
//        job.setReducerClass(CassandraReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SortedMapWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(List.class);
        
        job.setInputFormatClass(CqlPagingInputFormat.class);
        job.setOutputFormatClass(CqlOutputFormat.class);
        
//        ConfigHelper.setRangeBatchSize(getConf(), 99);
//        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "localhost");
        
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), "hikeyspace", inputColumnFamilyName);

        String columnName = "iso2";
        SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes(columnName)));
        ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "hikeyspace", outputColumnFamilyName);

//        job.setNumReduceTasks(1);
        
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        AnalyzerBeansConfiguration analyzerBeansConfiguration = SampleCassandraConfiguration
                .buildAnalyzerBeansConfiguration();
        AnalysisJob analysisJob = SampleCassandraConfiguration.buildAnalysisJob(analyzerBeansConfiguration);
        CassandraTool cassandraTool = new CassandraTool(analyzerBeansConfiguration, analysisJob);
        ToolRunner.run(cassandraTool, args);
    }

}
