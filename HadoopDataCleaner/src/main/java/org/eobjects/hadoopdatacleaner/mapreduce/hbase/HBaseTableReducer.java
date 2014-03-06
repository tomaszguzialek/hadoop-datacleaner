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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.eobjects.hadoopdatacleaner.datastores.hbase.utils.ResultUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableReducer extends
        TableReducer</* KEYIN */Text, /* VALUEIN */SortedMapWritable, /* VALUEOUT */KeyValue> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableReducer.class);

    public void reduce(Text analyzerKey, Iterable<SortedMapWritable> writableResults, Context context)
            throws IOException, InterruptedException {

        logger.info("analyzerKey = " + analyzerKey.toString() + " rows: ");
        for (SortedMapWritable rowWritable : writableResults) {
            List<KeyValue> keyValues = new ArrayList<KeyValue>();
            for (@SuppressWarnings("rawtypes")
            Map.Entry<WritableComparable, Writable> rowEntry : rowWritable.entrySet()) {
                Text columnFamilyAndName = (Text) rowEntry.getKey();
                Text columnValue = (Text) rowEntry.getValue();
                String[] split = columnFamilyAndName.toString().split(":");
                String columnFamily = split[0];
                String columnName = split[1];
                KeyValue keyValue = new KeyValue(Bytes.toBytes(columnValue.toString()), Bytes.toBytes(columnFamily),
                        Bytes.toBytes(columnName), Bytes.toBytes(columnValue.toString()));
                keyValues.add(keyValue);
            }
            Result result = new Result(keyValues);
            ResultUtils.printResult(result, logger);
            Put put = ResultUtils.preparePut(result);
            context.write(null, put);
        }
        logger.info("end of analyzerKey = " + analyzerKey.toString() + " rows.");
    }
}
