package com.xdl.car2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
//4200214112 雷冰冰
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MessageMap extends Mapper<Object,Text,Text,IntWritable> {

	private Text outkey = new Text();
	private IntWritable outvalue = new IntWritable();
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] str=value.toString().trim().split(",");
		String owner = str[7];
		String model =str[3];
		String type =str[5];
		outkey.set(owner+","+model+","+type);	
		
		if(str!=null &&	str.length==39 && str[5].trim()!=null && str[8].trim()!=null && str[7].trim()!=null)
		{
			//4200214112 雷冰冰
			outvalue.set(1);
			context.write(outkey,outvalue);
		}
	}
	

}
