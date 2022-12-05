package com.xdl.car3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class PriceDriver {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		  
		 Job job=Job.getInstance(conf,PriceDriver.class.getName());
		  job.setJarByClass(PriceDriver.class);
		  
		  job.setMapperClass(PriceMapper.class);
		  job.setReducerClass(PriceReducer.class);
		  
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  
		  FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/car/carSales1 .csv"));
		  FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/result/car10"));
		  job.waitForCompletion (true);
	}
}
