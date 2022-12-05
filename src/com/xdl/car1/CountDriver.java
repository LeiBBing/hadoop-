package com.xdl.car1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, com.xdl.car1.CountDriver.class.getName());
		job1.setJarByClass(com.xdl.car1.CountDriver.class);
		job1.setMapperClass(com.xdl.car1.CountMap.class);

		job1.setReducerClass(com.xdl.car1.CountReduce.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);
		// TODO: specify output types
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		//4200214112 雷冰冰
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job1, new Path("hdfs://master:9000/car/carSales.csv"));
		FileOutputFormat.setOutputPath(job1, new Path("hdfs://master:9000/result/car1"));
		if (!job1.waitForCompletion(true))
			return;
	}

}