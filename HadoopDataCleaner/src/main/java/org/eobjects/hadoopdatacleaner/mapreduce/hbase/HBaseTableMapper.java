package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.eobjects.hadoopdatacleaner.datastores.hbase.utils.ResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableMapper extends TableMapper</* KEYIN */ImmutableBytesWritable, /* KEYOUT */Result> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableMapper.class);

    public void map(/* KEYOUT */ImmutableBytesWritable row, /* KEYOUT */Result result, Context context)
            throws InterruptedException, IOException {

        ResultUtils.printResult(result, logger);
        
        KeyValue[] raw = result.raw();
        byte[] value = raw[0].split().getValue();
       
      
        if (Bytes.toString(value).equals("Denmark")) {
            context.write(AnalyzerGroupKeys.STRING_ANALYZER.getWritableKey(), result);
        } else {
            context.write(AnalyzerGroupKeys.VALUE_DISTRIBUTION_ANALYZER.getWritableKey(), result);
        }
    }

}