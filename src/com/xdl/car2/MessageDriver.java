package com.xdl.car2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MessageDriver {
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		  
		 Job job=Job.getInstance(conf,MessageDriver.class.getName());
		  job.setJarByClass(MessageDriver.class);
		  
		  job.setMapperClass(MessageMap.class);
		  job.setReducerClass(MessageReducer.class);
		//4200214112 雷冰冰
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		//4200214112 雷冰冰
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  
		  FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/car/carSales.csv"));
		  FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/result/car6"));
		  job.waitForCompletion (true);
}
}
