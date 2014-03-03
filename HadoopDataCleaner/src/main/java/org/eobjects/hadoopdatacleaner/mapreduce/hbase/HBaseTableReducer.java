package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.eobjects.hadoopdatacleaner.datastores.hbase.utils.ResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableReducer extends
        TableReducer</* KEYIN */ImmutableBytesWritable, /* VALUEIN */Result, /* VALUEOUT */KeyValue> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableReducer.class);

    public void reduce(ImmutableBytesWritable rowKey, Iterable<Result> results, Context context) throws IOException,
            InterruptedException {

        logger.info("AnalyzerGroupKey: " + Bytes.toString(rowKey.get()));
        for (Result result : results) {
            ResultUtils.printResult(result, logger);
            Put put = ResultUtils.preparePut(result);
            context.write(null, put);
        }
        logger.info("AnalyzerGroupKey: " + Bytes.toString(rowKey.get()));
    }
}
