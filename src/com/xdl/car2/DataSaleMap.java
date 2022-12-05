package com.xdl.car2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataSaleMap extends Mapper<Object,Text,Text,IntWritable> {

	private Text outkey = new Text();
	private IntWritable outvalue = new IntWritable();
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] str=value.toString().trim().split(",");
		String month = str[4];
		String type = str[8];
		outkey.set(month+"月-"+type);
		outvalue.set(1);
		if(str!=null &&	str.length==39 && str[4]!=null && str[8]!=null )
		{
			context.write(outkey, outvalue);
		}
		
	}
}
