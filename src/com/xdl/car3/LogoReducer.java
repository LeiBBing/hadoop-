package com.xdl.car3;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogoReducer extends Reducer<Text,IntWritable,Text,Text> {
	
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable val : values)
		{
			count+=val.get();
		}
		context.write(key,new Text(""));
	}
	
	//4200214112 雷冰冰
}