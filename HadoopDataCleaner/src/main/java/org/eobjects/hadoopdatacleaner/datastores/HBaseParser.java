package org.eobjects.hadoopdatacleaner.datastores;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.eobjects.analyzer.data.InputRow;
import org.eobjects.analyzer.data.MockInputColumn;
import org.eobjects.analyzer.data.MockInputRow;

public class HBaseParser {

    public InputRow prepareRow(Result result) {
        MockInputRow row = new MockInputRow();
        for (KeyValue keyValue : result.raw()) {
            SplitKeyValue splitKeyValue = keyValue.split();
            byte[] family = splitKeyValue.getFamily();
            byte[] column = splitKeyValue.getQualifier();
            byte[] value = splitKeyValue.getValue();
            row.put(new MockInputColumn<String>(Bytes.toString(family) + ":" + Bytes.toString(column)),
                    Bytes.toString(value));
        }
        return row;
    }

}
