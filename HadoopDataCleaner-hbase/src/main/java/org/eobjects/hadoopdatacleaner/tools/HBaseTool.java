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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eobjects.hadoopdatacleaner.mapreduce.hbase.HBaseTableMapper;
import org.eobjects.hadoopdatacleaner.mapreduce.hbase.HBaseTableReducer;
import org.xml.sax.SAXException;

public final class HBaseTool extends HadoopDataCleanerTool implements Tool {

	public HBaseTool(String analysisJobXml, String inputTableName, String outputTableName) throws IOException,
			XPathExpressionException, ParserConfigurationException, SAXException {
        super(analysisJobXml, inputTableName, outputTableName);
    }

    public int run(String[] args) throws Exception {
        String inputTableName, outputTableName;
        if (args.length == 3) {
            inputTableName = args[1];
            outputTableName = args[2];
        } else {
            System.err.println("Incorrect number of arguments. Expected: <analysisJobPath> <inputTableName> <outputTableName>");
            return -1;
        }

        Configuration mapReduceConfiguration = HBaseConfiguration.create();
        mapReduceConfiguration.set(ANALYSIS_JOB_XML_KEY, analysisJobXml);
        mapReduceConfiguration.set(INPUT_TABLE_NAME_KEY, inputTableName);
        mapReduceConfiguration.set(OUTPUT_TABLE_NAME_KEY, outputTableName);

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
        
        TableMapReduceUtil.addDependencyJars(job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String analysisJobPath;
        String inputTableName;
        String outputTableName;
        if (args.length == 3) {
            analysisJobPath = args[0];
            inputTableName = args[1];
            outputTableName = args[2];

            String analysisJobXml = FileUtils.readFileToString(new File(analysisJobPath));
            HBaseTool hBaseTool = new HBaseTool(analysisJobXml, inputTableName, outputTableName);
            ToolRunner.run(hBaseTool, args);
        } else {
            System.err.println("Incorrect number of arguments. Expected: <analysisJobPath> <inputTableName> <outputTableName>");
        }
        
    }
    
}
