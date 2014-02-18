package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class HadoopDataCleanerReducer extends Reducer<LongWritable, SortedMapWritable, LongWritable, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<SortedMapWritable> rows, Context context) throws IOException,
            InterruptedException {
        Text finalText = new Text();
        for (SortedMapWritable row : rows) {
            for (@SuppressWarnings("rawtypes")
            Iterator<Entry<WritableComparable, Writable>> iterator = row.entrySet().iterator(); iterator.hasNext();) {
                Text value = ((Text) iterator.next().getValue());
                finalText.set(finalText.toString() + value.toString());
                if (iterator.hasNext())
                    finalText.set(finalText.toString() + ";");
                else
                    finalText.set(finalText.toString() + "\n");
            }
        }

        context.write(key, finalText);
    }

}
