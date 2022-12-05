package com.xdl.car1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MounthDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job2=Job.getInstance(conf,com.xdl.car1.MouthMap.class.getName());
		job2.setJarByClass(com.xdl.car1.MounthDriver.class);
		 job2.setMapperClass(com.xdl.car1.MouthMap.class);
		// TODO: specify a reducer
		 job2.setReducerClass(com.xdl.car1.MouthReduce.class);

		// TODO: specify output types
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job2, new Path("hdfs://master:9000/car/carSales1 .csv"));
		FileOutputFormat.setOutputPath(job2, new Path("hdfs://master:9000/result/car2"));
		//4200214112 雷冰冰
		if (!job2.waitForCompletion(true))
			return;
	}

}