package com.xdl.car2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReducer extends Reducer<Text, Text, Text, Text> {


	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		for(IntWritable value : values)
		{
			sum += value.get();
		}
		context.write(key,new Text(""+sum));
	}
	//4200214112 雷冰冰
}
