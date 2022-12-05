package com.xdl.car2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		  
		 Job job=Job.getInstance(conf,UserDriver.class.getName());
		  job.setJarByClass(UserDriver.class);
		  
		  job.setMapperClass(UserMap.class);
		  job.setReducerClass(UserReducer.class);
		  
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/car/carSales.csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/result/car8"));
		//4200214112 雷冰冰
		if (!job.waitForCompletion(true))
			return;
	}

}
