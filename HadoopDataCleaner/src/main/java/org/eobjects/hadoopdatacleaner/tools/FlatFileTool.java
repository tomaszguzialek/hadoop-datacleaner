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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.sample.SampleCsvConfiguration;
import org.eobjects.hadoopdatacleaner.mapreduce.flatfile.FlatFileMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.flatfile.FlatFileReducer;

public final class FlatFileTool extends HadoopDataCleanerTool implements Tool {

	public FlatFileTool(
			AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) throws FileNotFoundException {

	    super(analyzerBeansConfiguration, analysisJob);
	}

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 2) {
			input = args[0];
			output = args[1];
		} else {
			System.err
					.println("Incorrect number of arguments.  Expected: <input> output");
			return -1;
		}

		Configuration conf = getConf();
		conf.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);
		conf.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY,
				analyzerBeansConfigurationDatastores);

		return runMapReduceJob(input, output, conf);
	}

	private int runMapReduceJob(String analysisJobPath, String output,
			Configuration mapReduceConfiguration) throws IOException,
			InterruptedException, ClassNotFoundException {
	    
		Job job = Job.getInstance(mapReduceConfiguration);
		job.setJarByClass(FlatFileMapper.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(analysisJobPath));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(FlatFileMapper.class);
		job.setReducerClass(FlatFileReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SortedMapWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileSystem hdfs = FileSystem.get(mapReduceConfiguration);
		if (hdfs.exists(new Path(output)))
			hdfs.delete(new Path(output), true);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        AnalyzerBeansConfiguration analyzerBeansConfiguration = SampleCsvConfiguration.buildAnalyzerBeansConfigurationHdfs(args[0]);
        AnalysisJob analysisJob = SampleCsvConfiguration.buildAnalysisJob(analyzerBeansConfiguration);
        FlatFileTool hadoopDataCleanerTool = new FlatFileTool(analyzerBeansConfiguration, analysisJob);
        ToolRunner.run(hadoopDataCleanerTool, args);
    }

}
