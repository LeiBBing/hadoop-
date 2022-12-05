package com.xdl.car1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AreaDriver2 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job4=Job.getInstance(conf,com.xdl.car1.AreaDriver2.class.getName());
		job4.setJarByClass(com.xdl.car1.AreaDriver2.class);
		 job4.setMapperClass(com.xdl.car1.AreaMap2.class);
		// TODO: specify a reducer
		 job4.setReducerClass(com.xdl.car1.AreaReducer2.class);

		// TODO: specify output types
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(IntWritable.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job4, new Path("hdfs://master:9000/car/carSales1 .csv"));
		FileOutputFormat.setOutputPath(job4, new Path("hdfs://master:9000/result/car4"));

		if (!job4.waitForCompletion(true))
			return;
	}

}//4200214112 雷冰冰
