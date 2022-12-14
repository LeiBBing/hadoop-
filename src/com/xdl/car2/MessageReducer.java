package com.xdl.car2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MessageReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

	IntWritable sum = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count = 0;
		for(IntWritable val : values)
		{
			count+=val.get();
		}
		sum.set(count);
		context.write(key, sum);
		//4200214112 雷冰冰//4200214112 雷冰冰
	}
	

}
