package org.eobjects.hadoopdatacleaner.mapreduce.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.eobjects.hadoopdatacleaner.datastores.hbase.utils.ResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableMapper extends TableMapper</* KEYIN */ImmutableBytesWritable, /* KEYOUT */Result> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableMapper.class);

    public void map(/* KEYOUT */ImmutableBytesWritable row, /* KEYOUT */Result result, Context context)
            throws InterruptedException, IOException {

        ResultUtils.printResult(result, logger);
        context.write(row, result);
    }

}