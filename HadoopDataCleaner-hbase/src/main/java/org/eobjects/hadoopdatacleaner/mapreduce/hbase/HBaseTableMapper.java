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

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.hadoopdatacleaner.configuration.AnalyzerBeansConfigurationHelper;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.datastores.HBaseParser;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperDelegate;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter.Callback;
import org.eobjects.hadoopdatacleaner.tools.HadoopDataCleanerTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class HBaseTableMapper extends
		TableMapper</* KEYOUT */Text, /* VALUEOUT */SortedMapWritable> {

	@SuppressWarnings("unused")
	private static final Logger logger = LoggerFactory
			.getLogger(HBaseTableMapper.class);

	private HBaseParser hBaseParser;

	private MapperDelegate mapperDelegate;

	@Override
	protected void setup(
			org.apache.hadoop.mapreduce.Mapper</* KEYIN */ImmutableBytesWritable, /* VALUEIN */Result, /* KEYOUT */Text, /* VALUEOUT */SortedMapWritable>.Context context)
			throws IOException, InterruptedException {
		Configuration mapReduceConfiguration = context.getConfiguration();
		String analysisJobXml = mapReduceConfiguration
				.get(HadoopDataCleanerTool.ANALYSIS_JOB_XML_KEY);
		AnalyzerBeansConfiguration analyzerBeansConfiguration;
		try {
			analyzerBeansConfiguration = AnalyzerBeansConfigurationHelper
					.build(analysisJobXml);
			String datastoresConfigurationLines = ConfigurationSerializer
					.serializeAnalyzerBeansConfigurationDataStores(analyzerBeansConfiguration);
			mapperDelegate = new MapperDelegate(datastoresConfigurationLines,
					analysisJobXml);
		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hBaseParser = new HBaseParser(mapperDelegate.getAnalysisJob()
				.getSourceColumns());
		super.setup(context);
	}

	public void map(/* KEYIN */ImmutableBytesWritable row, /* VALUEIN */
			Result result, final Context context) throws InterruptedException,
			IOException {

		InputRow inputRow = hBaseParser.prepareRow(result);

		Callback mapperEmitterCallback = new MapperEmitter.Callback() {

			public void write(Text key, SortedMapWritable row)
					throws IOException, InterruptedException {
				context.write(key, row);
			}
		};

		mapperDelegate.run(inputRow, mapperEmitterCallback);
	}

}