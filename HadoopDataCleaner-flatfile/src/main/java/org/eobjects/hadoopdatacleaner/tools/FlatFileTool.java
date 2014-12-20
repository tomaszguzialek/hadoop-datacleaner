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

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
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
import org.eobjects.hadoopdatacleaner.mapreduce.flatfile.FlatFileMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.flatfile.FlatFileReducer;
import org.xml.sax.SAXException;

public final class FlatFileTool extends HadoopDataCleanerTool implements Tool {

	public FlatFileTool(String analysisJobXml, String inputTableName, String outputTableName) throws IOException,
			XPathExpressionException, ParserConfigurationException,
			SAXException {

		super(analysisJobXml, inputTableName, outputTableName);
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

		FileSystem fileSystem = FileSystem.get(mapReduceConfiguration);
		if (fileSystem.exists(new Path(output)))
			fileSystem.delete(new Path(output), true);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String analysisJobPath;
		String inputTableName;
		String outputTableName;
		if (args.length == 3) {
			analysisJobPath = args[0];
			
			inputTableName = new File(args[0]).getName();
			outputTableName = new File(args[1]).getName();
			
			String analysisJobXml = FileUtils.readFileToString(new File(
					analysisJobPath));
			FlatFileTool hadoopDataCleanerTool = new FlatFileTool(
					analysisJobXml, inputTableName, outputTableName);
			ToolRunner.run(hadoopDataCleanerTool, args);
		} else {
			System.err
					.println("Incorrect number of arguments. Expected: <analysisJobPath> <input> output");
		}
	}

}
