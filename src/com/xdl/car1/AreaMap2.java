package com.xdl.car1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AreaMap2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(
			Object key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>.Context context)
	        throws java.io.IOException,InterruptedException{
		String[] str=value.toString().trim().split("\t");
		if(str!=null&&str.length>1) {
			 String[]  shi= str[0].split(",");
			 if(shi !=null&&shi.length>1) {
				 context.write(new Text(shi[0]), new IntWritable(Integer.parseInt(str[4])));
			 }
	 
		}
	}

}
