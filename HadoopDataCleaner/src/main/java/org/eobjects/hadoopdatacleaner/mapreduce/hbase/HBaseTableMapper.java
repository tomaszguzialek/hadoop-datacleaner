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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.hadoopdatacleaner.datastores.HBaseParser;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperDelegate;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter.Callback;
import org.eobjects.hadoopdatacleaner.tools.FlatFileTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableMapper extends TableMapper</* KEYOUT */Text, /* VALUEOUT */SortedMapWritable> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HBaseTableMapper.class);

    private HBaseParser hBaseParser;

    private MapperDelegate mapperDelegate;

    @Override
    protected void setup(
            org.apache.hadoop.mapreduce.Mapper</* KEYIN */ImmutableBytesWritable, /* VALUEIN */Result, /* KEYOUT */Text, /* VALUEOUT */SortedMapWritable>.Context context)
            throws IOException, InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        mapperDelegate = new MapperDelegate(datastoresConfigurationLines, analysisJobXml);
        hBaseParser = new HBaseParser(mapperDelegate.getAnalysisJob().getSourceColumns());
        super.setup(context);
    }

    public void map(/* KEYIN */ImmutableBytesWritable row, /* VALUEIN */Result result, final Context context)
            throws InterruptedException, IOException {

        InputRow inputRow = hBaseParser.prepareRow(result);

        Callback mapperEmitterCallback = new MapperEmitter.Callback() {
            @Override
            public void write(Text key, SortedMapWritable row) throws IOException, InterruptedException {
                context.write(key, row);
            }
        };
        
        mapperDelegate.run(inputRow, mapperEmitterCallback);
    }

}