package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class HBaseTableMapper extends TableMapper<Text, Text> {

    private Text text = new Text();

    public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
        KeyValue[] keyValues = value.raw();

        for (int i = 0; i < keyValues.length; i++) {
            SplitKeyValue splitKeyValue = keyValues[i].split();
            // String val = new String(value.getValue(splitKeyValue.getFamily(),
            // splitKeyValue.getQualifier()));
            System.out.println("Family – " + Bytes.toString(splitKeyValue.getFamily()));
            System.out.println("Qualifier – " + Bytes.toString(splitKeyValue.getQualifier()));
            System.out.println("Key: " + Bytes.toString(splitKeyValue.getRow()) + ", Value: "
                    + Bytes.toString(splitKeyValue.getValue()));

            text.set(Bytes.toString(splitKeyValue.getRow()) + "|" + Bytes.toString(splitKeyValue.getValue()) + "|"
                    + Bytes.toString(splitKeyValue.getFamily()) + "|" + Bytes.toString(splitKeyValue.getQualifier()));
        }
    }

}