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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.hadoopdatacleaner.datastores.CsvParser;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperDelegate;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter.Callback;
import org.eobjects.hadoopdatacleaner.tools.FlatFileTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatFileMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(FlatFileMapper.class);

    private CsvParser csvParser;

    private MapperDelegate mapperDelegate;

    protected void setup(Mapper<LongWritable, Text, Text, SortedMapWritable>.Context context) throws IOException,
            InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        this.mapperDelegate = new MapperDelegate(datastoresConfigurationLines, analysisJobXml);
        csvParser = new CsvParser(mapperDelegate.getAnalysisJob().getSourceColumns(), ";");
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text csvLine, final Context context) throws IOException, InterruptedException {
        InputRow inputRow = csvParser.prepareRow(csvLine);

        Callback mapperEmitterCallback = new MapperEmitter.Callback() {

            public void write(Text key, SortedMapWritable row) throws IOException, InterruptedException {
                context.write(key, row);

            }
        };
        
        mapperDelegate.run(inputRow, mapperEmitterCallback);
    }
}
