package com.xdl.car1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMap extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,LongWritable>.Context context) 
			throws IOException, InterruptedException {
		String[] owns = value.toString().trim().split(",");
		if(null !=owns && owns.length > 10 && owns[11] !=null) {
			if(owns[10].equals("非营运")) {
				context.write(new Text("乘用车辆"), new LongWritable(1));
			}
			else {
				context.write(new Text("商用车辆"), new LongWritable(1));
			}
		}
	};

}
