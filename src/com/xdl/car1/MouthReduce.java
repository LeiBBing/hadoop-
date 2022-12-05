package com.xdl.car1;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MouthReduce extends Reducer<Text,IntWritable,Text,DoubleWritable>{
	 public Map<String,Integer> map = new HashMap<String,Integer>(); 
	 int alls=0;
	 public void reduce(Text key,java.lang.Iterable<IntWritable> value,
			 org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,
			 Text,DoubleWritable>.Context context) 
					 throws java.io.IOException,InterruptedException{
		 int count=0;
		 for(IntWritable con:value) {
			 count+=con.get();
		 }
		 alls+=count;
		 
		//4200214112 雷冰冰
		 map.put(key.toString(), count);
		 /*Set<String> keys = map.keySet();
		 for(String key1 : keys) {
			 int val=map.get(key1);
			 double percent=val*1.0/alls;
			 context.write(new Text(key1), new DoubleWritable(percent));
		 }	
		// context.write(new Text(key.toString()), new DoubleWritable(alls*1.0));*/
		 
	 };
	 //此处书本中的protected权限过低限制传参，产生报错，后面改成了public
	 public void cleanup(org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, 
			 Text, DoubleWritable>.Context context)
					 throws java.io.IOException,InterruptedException {
		 Set<String> keys = map.keySet();		 
		 for(String key1 : keys) {
			 int value=map.get(key1);
			 double percent=value*1.0/alls;
			 context.write(new Text(key1), new DoubleWritable(percent));
			 
			
		 }	     	 
	 };
	 
	 
}