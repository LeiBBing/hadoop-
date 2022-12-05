package com.xdl.car2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMap extends Mapper<Object,Text,Text,IntWritable> {


	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] str=value.toString().trim().split(",");
		if(str != null && str.length==39 && str[8]!=null && str[37]!=null && str[37].matches("^\\d*$") && str[38]!=null)
		{
			int age =2022-Integer.parseInt(str[37]);
			int range1 = age/10*10;
			int range2 = range1+10;
			context.write(new Text(str[8]+","+(range1+"--"+range2)+","+str[38]), new IntWritable(1));
			//4200214112 雷冰冰
		}
		
		
	}
	

}
