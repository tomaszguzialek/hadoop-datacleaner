package org.eobjects.hadoopdatacleaner.datastores.hbase.utils;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

public class ResultUtils {

    public static void printResult(Result result, Logger logger) {
        logger.info("Row: ");
        for (KeyValue keyValue : result.raw()) {
            SplitKeyValue splitKeyValue = keyValue.split();
            byte[] family = splitKeyValue.getFamily();
            byte[] column = splitKeyValue.getQualifier();
            byte[] value = splitKeyValue.getValue();
            logger.info("\t" + Bytes.toString(family) + ":" + Bytes.toString(column) + " = " + Bytes.toString(value));
        }
    }
    
    public static Put preparePut(Result result) {
        Put put = new Put(result.getRow());
        for (KeyValue keyValue : result.raw()) {
            SplitKeyValue splitKeyValue = keyValue.split();
            byte[] family = splitKeyValue.getFamily();
            byte[] column = splitKeyValue.getQualifier();
            byte[] value = splitKeyValue.getValue();
            put.add(family, column, value);
        }
        return put;
    }
}
