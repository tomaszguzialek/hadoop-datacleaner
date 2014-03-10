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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.eobjects.hadoopdatacleaner.datastores.RowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTableReducer extends
        TableReducer</* KEYIN */Text, /* VALUEIN */SortedMapWritable, /* KEYOUT */NullWritable> {

    private static final Logger logger = LoggerFactory.getLogger(HBaseTableReducer.class);

    public void reduce(Text analyzerKey, Iterable<SortedMapWritable> writableResults, Context context)
            throws IOException, InterruptedException {

        logger.info("analyzerKey = " + analyzerKey.toString() + " rows: ");
        for (SortedMapWritable rowWritable : writableResults) {
            Result result = RowUtils.sortedMapWritableToResult(rowWritable);
            RowUtils.printResult(result, logger);
            Put put = RowUtils.preparePut(result);
            context.write(NullWritable.get(), put);
        }
        logger.info("end of analyzerKey = " + analyzerKey.toString() + " rows.");
    }
}
