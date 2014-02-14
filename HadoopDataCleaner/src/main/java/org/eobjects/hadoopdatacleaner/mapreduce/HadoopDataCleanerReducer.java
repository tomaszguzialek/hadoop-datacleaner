package org.eobjects.hadoopdatacleaner.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.eobjects.hadoopdatacleaner.mapreduce.writables.TextArrayWritable;

public class HadoopDataCleanerReducer extends
		Reducer<LongWritable, TextArrayWritable, LongWritable, Text> {

	@Override
	public void reduce(LongWritable key, Iterable<TextArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		Text finalResult = new Text();
		
		Writable[] content = values.iterator().next().get();
		for (Writable writable : content) {
			finalResult = (Text) writable;
			break;
		}
		
//		for (TextArrayWritable textArrayWritable : values) {
//			Writable[] writables = textArrayWritable.get();
//			for (Writable writable : writables) {
//				Text text = (Text) writable;
//				finalResult = new Text(finalResult.toString() + text.toString());
//			}
//			break;
//		}
		context.write(key, finalResult);
	}

}
