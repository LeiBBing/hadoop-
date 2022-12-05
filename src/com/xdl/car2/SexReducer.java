package com.xdl.car2;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

	public class SexReducer extends Reducer<Text,IntWritable,Text,Text> {
	Map<String, Integer> map = new HashMap<String, Integer>();
	int sum = 0;
	public void reduce(Text key,
			java.lang.Iterable<IntWritable> values,
			org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,Text>.Context context) 
			throws  java.io.IOException,InterruptedException {
			int count=0;
			for(IntWritable val:values) {
				count+=val.get();
		 }
		 //男女购买车辆的总数
			sum += count;
		 //男女性分别购买车辆的数量
			map.put(key.toString(),count);
	};//4200214112 雷冰冰
	public void cleanup(org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,Text>.Context context)
			throws java.io.IOException,InterruptedException {
		 Set<String> keySet = map.keySet();
		 for(String key :keySet) {
			 int value = map.get(key);
			 double percent = value *1.0/sum;
			 context.write(new Text(key),new Text(""+percent));
		 } 
	}

};