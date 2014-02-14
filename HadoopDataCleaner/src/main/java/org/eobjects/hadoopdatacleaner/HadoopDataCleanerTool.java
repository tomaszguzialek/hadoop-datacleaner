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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.mapreduce.HadoopDataCleanerMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.HadoopDataCleanerReducer;
import org.eobjects.hadoopdatacleaner.mapreduce.writables.TextArrayWritable;

public final class HadoopDataCleanerTool extends Configured implements Tool {

	public final static String ANALYSIS_JOB_XML_KEY = "analysis.job.xml";
	public final static String ANALYZER_BEANS_CONFIGURATION_DATASTORES_CSV_KEY = "analyzer.beans.configuration.datastores.csv.key";

	private String analysisJobXml;

	private String analyzerBeansConfigurationDatastoresCsv;

	public HadoopDataCleanerTool(
			AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) throws FileNotFoundException {

		this.analyzerBeansConfigurationDatastoresCsv = ConfigurationSerializer.serializeAnalyzerBeansConfigurationToCsv(analyzerBeansConfiguration);
		this.analysisJobXml = ConfigurationSerializer.serializeAnalysisJobToXml(analyzerBeansConfiguration, analysisJob);
	}

	@Override
	public int run(String[] args) throws Exception {
		String analysisJobPath, output;
		if (args.length == 2) {
			analysisJobPath = args[0];
			output = args[1];
		} else {
			System.err
					.println("Incorrect number of arguments.  Expected: <path_to_analysis_xml_file> output");
			return -1;
		}

		Configuration conf = getConf();
		conf.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);
		conf.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_CSV_KEY,
				analyzerBeansConfigurationDatastoresCsv);

		return runMapReduceJob(analysisJobPath, output, conf);
	}

	private int runMapReduceJob(String analysisJobPath, String output,
			Configuration mapReduceConfiguration) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(mapReduceConfiguration);
		job.setJarByClass(HadoopDataCleanerMapper.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(analysisJobPath));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(HadoopDataCleanerMapper.class);
		job.setReducerClass(HadoopDataCleanerReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(TextArrayWritable.class);

		FileSystem hdfs = FileSystem.get(mapReduceConfiguration);
		if (hdfs.exists(new Path(output)))
			hdfs.delete(new Path(output), true);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
