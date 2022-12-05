package com.xdl.car3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PriceMapper extends Mapper<Object,Text,Text,IntWritable> {

	private Text outkey = new Text();
	private IntWritable outvalue = new IntWritable();
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] str=value.toString().trim().split(",");
		String output = str[13];
		String type = str[7];
		outkey.set(type +","+output);
		outvalue.set(1);
		if(str!=null && str.length==39 && str[7]!=null && str[13]!=null  )
		{
			context.write(outkey, outvalue);
		}
	}
	//4200214112 雷冰冰
}
