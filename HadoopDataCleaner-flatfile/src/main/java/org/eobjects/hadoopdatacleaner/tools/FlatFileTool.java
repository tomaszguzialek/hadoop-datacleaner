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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

	public static final Log LOG = LogFactory.getLog(FlatFileTool.class);

	public FlatFileTool(AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) throws FileNotFoundException {

		super(analyzerBeansConfiguration, analysisJob);
	}

	public FlatFileTool(AnalyzerBeansConfiguration analyzerBeansConfiguration,
			String analysisJobXml) throws IOException {

		super(analyzerBeansConfiguration, analysisJobXml);
	}

	public int run(String[] args) throws Exception {
		String input, output;
		if (args.length == 3) {
			input = args[1];
			output = args[2];
		} else {
			System.err
					.println("Incorrect number of arguments. Expected: <analysisJobPath> <input> output");
			return -1;
		}

		Configuration conf = getConf();
		conf.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);
		conf.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY,
				analyzerBeansConfigurationDatastores);

		return runMapReduceJob(input, output, conf);
	}

	private int runMapReduceJob(String input, String output,
			Configuration mapReduceConfiguration) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(mapReduceConfiguration);
		job.setJarByClass(FlatFileMapper.class);
		job.setJobName(this.getClass().getName());

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(FlatFileMapper.class);
		job.setReducerClass(FlatFileReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SortedMapWritable.class);

		job.setNumReduceTasks(1);

		// TODO externalize to args?
		mapReduceConfiguration.addResource(new Path(
				"/etc/hadoop/conf/core-site.xml"));

		FileSystem fileSystem = FileSystem.get(mapReduceConfiguration);
		System.out.println("\n\n");
		System.out.println(mapReduceConfiguration.getRaw("fs.default.name"));
		System.out.println(mapReduceConfiguration.getRaw("fs.defaultFS"));
		System.out.println("The fileSystem is: " + fileSystem.toString());
		System.out.println("\n\n");
		if (fileSystem.exists(new Path(output)))
			fileSystem.delete(new Path(output), true);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();

		String analysisJobPath, input;
		if (args.length == 3) {
			analysisJobPath = args[0];
			input = args[1];

			AnalyzerBeansConfiguration analyzerBeansConfiguration = SampleCsvConfiguration
					.buildAnalyzerBeansConfiguration(input);

			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.getLocal(conf);

			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(fileSystem.open(new Path(
							analysisJobPath))));
			String line = bufferedReader.readLine();

			while (line != null) {
				System.out.println(line);
				line = bufferedReader.readLine();
			}

			String analysisJobXml = FileUtils.readFileToString(new File(
					analysisJobPath));
			FlatFileTool hadoopDataCleanerTool = new FlatFileTool(
					analyzerBeansConfiguration, analysisJobXml);
			ToolRunner.run(hadoopDataCleanerTool, args);
		} else {
			System.err
					.println("Incorrect number of arguments. Expected: <analysisJobPath> <input> output");
		}
		long stop = System.nanoTime();
		long executionTime = stop - start;
		System.out.println("Execution time: "
				+ TimeUnit.SECONDS.convert(executionTime, TimeUnit.NANOSECONDS)
				+ " (start: " + start + ", stop: " + stop + ")");
		LOG.info("Execution time: "
				+ TimeUnit.SECONDS.convert(executionTime, TimeUnit.NANOSECONDS)
				+ " (start: " + start + ", stop: " + stop + ")");
	}

}
