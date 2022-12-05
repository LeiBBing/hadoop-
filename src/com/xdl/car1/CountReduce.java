package com.xdl.car1;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {
	Map<String,Long> maps = new HashMap<String, Long>();
	double all=0;
	public void reduce(Text key, java.lang.Iterable<LongWritable> values, 
			org.apache.hadoop.mapreduce.Reducer<Text,LongWritable,Text,DoubleWritable>.Context context) 
					throws java.io.IOException, InterruptedException {
		// process values
		long sum=0;
		for(LongWritable val : values) {
			sum += val.get();
		}
		all +=sum;
		maps.put(key.toString(),sum);
	};
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text, LongWritable,Text,DoubleWritable>.Context context) 
			throws java.io.IOException, InterruptedException {
		// process values
		Set<String> keySet =maps.keySet();
		for(String str : keySet){
			long value=maps.get(str);
			double percent=value/all;
			context.write(new Text(str),new DoubleWritable(percent));
		}
	}

}
