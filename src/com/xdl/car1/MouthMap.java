package com.xdl.car1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MouthMap extends Mapper<Object,Text,Text,IntWritable>{
	public void map(
			Object key,
			Text value,
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>.Context context)
	        throws java.io.IOException,InterruptedException{
		String[] str=value.toString().trim().split(",");
		if(str!=null&&str[4]!=null) {
			 context.write(new Text(str[4]), 
					 new IntWritable(Integer.parseInt(str[11])));
	 
		}
	}
}
