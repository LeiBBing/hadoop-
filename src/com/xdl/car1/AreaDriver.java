package com.xdl.car1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AreaDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job3=Job.getInstance(conf,com.xdl.car1.AreaDriver.class.getName());
		job3.setJarByClass(com.xdl.car1.AreaDriver.class);
		 job3.setMapperClass(com.xdl.car1.AreaMap.class);
		// TODO: specify a reducer
		 job3.setReducerClass(com.xdl.car1.AreaReducer.class);

		// TODO: specify output types
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job3, new Path("hdfs://master:9000/car/carSales1 .csv"));
		FileOutputFormat.setOutputPath(job3, new Path("hdfs://master:9000/result/car3"));

		
		if (!job3.waitForCompletion(true))
			return;
	}


}
