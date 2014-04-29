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
package org.eobjects.hadoopdatacleaner.mapreduce.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.hadoopdatacleaner.datastores.CassandraParser;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperDelegate;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter;
import org.eobjects.hadoopdatacleaner.mapreduce.MapperEmitter.Callback;
import org.eobjects.hadoopdatacleaner.tools.FlatFileTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, SortedMapWritable> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CassandraMapper.class);

    CassandraParser cassandraParser;
    
    private MapperDelegate mapperDelegate;

    protected void setup(Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, SortedMapWritable>.Context context) throws IOException,
            InterruptedException {
        Configuration mapReduceConfiguration = context.getConfiguration();
        String datastoresConfigurationLines = mapReduceConfiguration
                .get(FlatFileTool.ANALYZER_BEANS_CONFIGURATION_DATASTORES_KEY);
        String analysisJobXml = mapReduceConfiguration.get(FlatFileTool.ANALYSIS_JOB_XML_KEY);
        this.mapperDelegate = new MapperDelegate(datastoresConfigurationLines, analysisJobXml);
        cassandraParser = new CassandraParser(mapperDelegate.getAnalysisJob().getSourceColumns());
        super.setup(context);
    }

    @Override
    public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, final Context context) throws IOException, InterruptedException {
        InputRow inputRow = cassandraParser.prepareRow(columns);
        
//        Callback mapperEmitterCallback = new MapperEmitter.Callback() {
//
//            @Override
//            public void write(Text key, SortedMapWritable row) throws IOException, InterruptedException {
//                context.write(key, row);
//
//            }
//        };
//        
//        mapperDelegate.run(inputRow, mapperEmitterCallback);
        
        context.write(new Text("testKey1"), new SortedMapWritable());
    }
}
