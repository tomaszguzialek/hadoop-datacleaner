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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

	public static final Log LOG = LogFactory.getLog(HBaseTool.class);

	public HBaseTool(AnalyzerBeansConfiguration analyzerBeansConfiguration,
			AnalysisJob analysisJob) {
		super(analyzerBeansConfiguration, analysisJob);
	}

	public HBaseTool(AnalyzerBeansConfiguration analyzerBeansConfiguration,
			String analysisJobXml) throws IOException {
		super(analyzerBeansConfiguration, analysisJobXml);
	}

	public int run(String[] args) throws Exception {
		String inputTableName, outputTableName, host;
		if (args.length == 4) {
			inputTableName = args[1];
			outputTableName = args[2];
			host = args[3];
		} else {
			System.err
					.println("Incorrect number of arguments. Expected: <analysisJobPath> <inputTableName> <outputTableName> <masterHostname>");
			return -1;
		}

		Configuration mapReduceConfiguration = HBaseConfiguration.create();
		mapReduceConfiguration.set("hbase.zookeeper.quorum", host);
		mapReduceConfiguration.set("hbase.zookeeper.property.clientPort",
				"2181");
		mapReduceConfiguration.set("hbase.rootdir", "hdfs://" + host
				+ ":8020/hbase");
		mapReduceConfiguration.set("hbase.client.max.perregion.tasks", "4");
		mapReduceConfiguration.set("mapreduce.tasktracker.map.tasks.maximum",
				"16");
		mapReduceConfiguration.set(ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY,
				analyzerBeansConfigurationDatastores);
		mapReduceConfiguration.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);

		return runMapReduceJob(inputTableName, outputTableName,
				mapReduceConfiguration);
	}

	private int runMapReduceJob(String inputTableName, String outputTableName,
			Configuration mapReduceConfiguration) throws IOException,
			InterruptedException, ClassNotFoundException {

		Job job = Job.getInstance(mapReduceConfiguration);
		job.setJarByClass(HBaseTableMapper.class);
		job.setJobName(this.getClass().getName());
		job.setNumReduceTasks(4);

		Scan scan = new Scan();
//		scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
//				inputTableName.getBytes());
		scan.setCaching(500); // 1 is the default in Scan, which will be bad
								// for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		TableMapReduceUtil.initTableMapperJob(inputTableName, scan, // Scan
																	// instance
																	// to
																	// control
																	// CF and
																	// attribute
																	// selection
				HBaseTableMapper.class, // mapper
				Text.class, // mapper output key
				SortedMapWritable.class, // mapper output value
				job);

		TableMapReduceUtil.initTableReducerJob(outputTableName, // output HBase
																// table name
				HBaseTableReducer.class, // reducer class
				job);

		TableMapReduceUtil.addDependencyJars(job);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		long start = System.nanoTime();

		String analysisJobPath;
		if (args.length == 4) {
			analysisJobPath = args[0];

			AnalyzerBeansConfiguration analyzerBeansConfiguration = SampleHBaseConfiguration
					.buildAnalyzerBeansConfiguration();
			String analysisJobXml = FileUtils.readFileToString(new File(
					analysisJobPath));
			HBaseTool hBaseTool = new HBaseTool(analyzerBeansConfiguration,
					analysisJobXml);
			ToolRunner.run(hBaseTool, args);
		} else {
			System.err
					.println("Incorrect number of arguments. Expected: <analysisJobPath> <inputTableName> <outputTableName> <masterHostname>");
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
