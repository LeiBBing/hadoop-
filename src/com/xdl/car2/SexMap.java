package com.xdl.car2;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SexMap extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key,
	Text value,
	org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>.Context context) 
			throws java.io.IOException,InterruptedException{
		String[] str=value.toString().trim().split(",");
		//根据性别过滤出购买汽车的记录
		
		if(str!=null&&str.length==39&&str[38]!=null&&(str[38].equals("男性")||str[38].equals("女性"))) {
			//key为性别男或女，value值为1
			 context.write(new Text(str[38]), new IntWritable(1));
		}
		//4200214112 雷冰冰//4200214112 雷冰冰

	}

}
