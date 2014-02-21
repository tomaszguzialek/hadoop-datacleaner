package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

public class HBaseTableReducer extends
        TableReducer</* KEYIN */ImmutableBytesWritable, /* VALUEIN */ Result, /* VALUEOUT */KeyValue> {
    public static final byte[] COLUMN_FAMILY = "mainFamily".getBytes();
    public static final byte[] COUNTRY_NAME = "country_name".getBytes();
    public static final byte[] ISO2 = "iso2".getBytes();
    public static final byte[] ISO3 = "iso3".getBytes();

    public void reduce(ImmutableBytesWritable rowKey, Iterable<Result> results, Context context)
            throws IOException, InterruptedException {
        
        for (Result result : results) {
            Put put = new Put(result.getValue(COLUMN_FAMILY, COUNTRY_NAME));
            put.add(COLUMN_FAMILY, COUNTRY_NAME, result.getValue(COLUMN_FAMILY, COUNTRY_NAME));
            put.add(COLUMN_FAMILY, ISO2, result.getValue(COLUMN_FAMILY, ISO2));
            put.add(COLUMN_FAMILY, ISO3, result.getValue(COLUMN_FAMILY, ISO3));
            context.write(null, put);
        }
    }
}
