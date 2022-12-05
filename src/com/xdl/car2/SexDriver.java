package com.xdl.car2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SexDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job5=Job.getInstance(conf,com.xdl.car2.SexDriver.class.getName());
		job5.setJarByClass(com.xdl.car2.SexDriver.class);
		 job5.setMapperClass(com.xdl.car2.SexMap.class);
		// TODO: specify a reducer
		 job5.setReducerClass(com.xdl.car2.SexReducer.class);

		// TODO: specify output types
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(IntWritable.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job5, new Path("hdfs://master:9000/car/carSales1 .csv"));
		FileOutputFormat.setOutputPath(job5, new Path("hdfs://master:9000/result/car5"));
		//4200214112 雷冰冰//4200214112 雷冰冰
		
		if (!job5.waitForCompletion(true))
			return;
	}


}
