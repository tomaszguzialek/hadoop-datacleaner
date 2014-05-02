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
package org.eobjects.hadoopdatacleaner.datastores;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.eobjects.analyzer.data.InputColumn;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputRow;
import org.slf4j.Logger;

public class RowUtils {

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

    public static void printSortedMapWritable(SortedMapWritable row, Logger logger) {
        logger.info("Row: ");
        for (@SuppressWarnings("rawtypes")
        Map.Entry<WritableComparable, Writable> entry : row.entrySet()) {
            Text columnName = (Text) entry.getKey();
            if (entry.getValue() instanceof Text) {
                Text columnValue = (Text) entry.getValue();
                logger.info("\t" + columnName + " = " + columnValue);
            } else {
                logger.info("\t" + columnName + " = " + null);
            }
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

    public static SortedMapWritable inputRowToSortedMapWritable(InputRow inputRow) {
        SortedMapWritable rowWritable = new SortedMapWritable();
        for (InputColumn<?> inputColumn : inputRow.getInputColumns()) {
            String columnName = inputColumn.getName();
            Object value = inputRow.getValue(inputColumn);
            if (value != null)
                rowWritable.put(new Text(columnName), new Text(value.toString()));
            else
                rowWritable.put(new Text(columnName), NullWritable.get());
        }
        return rowWritable;
    }

    public static InputRow sortedMapWritableToInputRow(SortedMapWritable rowWritable,
            Collection<InputColumn<?>> sourceColumns) {
        MockInputRow inputRow = new MockInputRow();

        for (@SuppressWarnings("rawtypes")
        Map.Entry<WritableComparable, Writable> rowEntry : rowWritable.entrySet()) {
            Text columnName = (Text) rowEntry.getKey();
            if (rowEntry.getValue() instanceof Text) {
                Text columnValue = (Text) rowEntry.getValue();
                for (InputColumn<?> sourceColumn : sourceColumns) {
                    String sourceColumnName = sourceColumn.getName();
                    if (sourceColumnName.equals(columnName.toString())) {
                        inputRow.put(sourceColumn, columnValue.toString());
                        break;
                    }
                }
            } else {
                for (InputColumn<?> sourceColumn : sourceColumns) {
                    String sourceColumnName = sourceColumn.getName();
                    if (sourceColumnName.equals(columnName.toString())) {
                        inputRow.put(sourceColumn, null);
                        break;
                    }
                }
            }
        }

        return inputRow;
    }

    public static Result sortedMapWritableToResult(SortedMapWritable row) {
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        for (@SuppressWarnings("rawtypes")
        Map.Entry<WritableComparable, Writable> rowEntry : row.entrySet()) {
            Text columnFamilyAndName = (Text) rowEntry.getKey();
            Text columnValue = (Text) rowEntry.getValue();
            String[] split = columnFamilyAndName.toString().split(":");
            String columnFamily = split[0];
            String columnName = split[1];
            KeyValue keyValue = new KeyValue(Bytes.toBytes(columnValue.toString()), Bytes.toBytes(columnFamily),
                    Bytes.toBytes(columnName), Bytes.toBytes(columnValue.toString()));
            keyValues.add(keyValue);
        }
        return new Result(keyValues);
    }

}
