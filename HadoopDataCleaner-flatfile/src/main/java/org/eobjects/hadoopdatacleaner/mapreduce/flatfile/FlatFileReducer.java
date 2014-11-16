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
package org.eobjects.hadoopdatacleaner.mapreduce.flatfile;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.eobjects.analyzer.beans.api.Analyzer;
import org.eobjects.analyzer.configuration.AnalyzerBeansConfiguration;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.job.AnalysisJob;
import org.eobjects.analyzer.result.AnalyzerResult;
import org.eobjects.hadoopdatacleaner.configuration.AnalyzerBeansConfigurationHelper;
import org.eobjects.hadoopdatacleaner.configuration.ConfigurationSerializer;
import org.eobjects.hadoopdatacleaner.datastores.CsvParser;
import org.eobjects.hadoopdatacleaner.datastores.RowUtils;
import org.eobjects.hadoopdatacleaner.tools.FlatFileTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class FlatFileReducer extends Reducer<Text, SortedMapWritable, NullWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(FlatFileReducer.class);

    private AnalyzerBeansConfiguration analyzerBeansConfiguration;

    private AnalysisJob analysisJob;

    protected void setup(Reducer<Text, SortedMapWritable, NullWritable, Text>.Context context) throws IOException,
            InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        try {
			analyzerBeansConfiguration = AnalyzerBeansConfigurationHelper.build(analysisJobXml);
			analysisJob = ConfigurationSerializer.deserializeAnalysisJobFromXml(analysisJobXml, analyzerBeansConfiguration);
			super.setup(context);
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
    }

    @Override
    public void reduce(Text analyzerKey, Iterable<SortedMapWritable> rows, Context context) throws IOException,
            InterruptedException {

        Analyzer<?> analyzer = ConfigurationSerializer.initializeAnalyzer(analyzerKey.toString(),
                analyzerBeansConfiguration, analysisJob);

        logger.info("analyzerKey = " + analyzerKey.toString() + " rows: ");
        for (SortedMapWritable rowWritable : rows) {
            InputRow inputRow = RowUtils.sortedMapWritableToInputRow(rowWritable, analysisJob.getSourceColumns());
            analyzer.run(inputRow, 1);

            logger.debug(RowUtils.sortedMapWritableToString(rowWritable));

            Text finalText = CsvParser.toCsvText(rowWritable);
            context.write(NullWritable.get(), finalText);
        }
        logger.info("end of analyzerKey = " + analyzerKey.toString() + " rows.");

        AnalyzerResult analyzerResult = analyzer.getResult();
        logger.debug("analyzerResult.toString(): " + analyzerResult.toString());
    }

}
