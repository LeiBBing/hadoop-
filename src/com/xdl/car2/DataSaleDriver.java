package com.xdl.car2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataSaleDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		  conf.set("dfs.replication", "1");
		  Job job = Job.getInstance(conf);
		  
		  job.setJarByClass(DataSaleDriver.class);
		  job.setMapperClass(DataSaleMap.class);
		  job.setReducerClass(DataSaleReducer.class);
		//4200214112 雷冰冰
		  job.setPartitionerClass(DataSalePartitioner.class);
		  job.setNumReduceTasks(12);
		  
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
		  
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/car/carSales1 .csv"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/result/car7"));
//4200214112 雷冰冰
		if (!job.waitForCompletion(true))
			return;
	}

}
