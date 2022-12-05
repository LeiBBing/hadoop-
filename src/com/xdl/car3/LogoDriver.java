package com.xdl.car3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogoDriver {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		  
		 Job job=Job.getInstance(conf,LogoDriver.class.getName());
		  job.setJarByClass(LogoDriver.class);
		  
		  job.setMapperClass(LogoMap.class);
		  job.setReducerClass(LogoReducer.class);
		  
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		//4200214112 雷冰冰
		  FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/car/carSales1 .csv"));
		  FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/result/car9"));
		  job.waitForCompletion (true);
	}
}
