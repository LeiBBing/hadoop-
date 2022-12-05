package com.xdl.car2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class DataSaleReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	private IntWritable otv = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text,
			IntWritable, 
			Text, 
			IntWritable>.Context context) 
					throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable value: values)
		{
			sum += value.get() ;
			
		}
		otv.set(sum);
		context.write(key, otv);
		
	}
	
	
	
}
class DataSalePartitioner extends Partitioner<Text,IntWritable> {
	@Override
	public int getPartition(Text text, IntWritable intwritable, int num) {
		String data= text.toString().substring(0,3);
		//4200214112 雷冰冰
		  if("1月-".equals(data))
		  {
		   return 0;
		  }
		  else if("2月-".equals(data))
		  {
			  return 1;
		  }
		  else if("3月-".equals(data))
		  {
			  return 2;
		  }
		  else if("4月-".equals(data))
		  {
			  return 3;
		  }
		  else if("5月-".equals(data))
		  {
			  return 4;
		  }
		  else if("6月-".equals(data))
		  {
			  return 5;
		  }
		  else if("7月-".equals(data))
		  {
			  return 6;
		  }
		  else if("8月-".equals(data))
		  {
			  return 7;
		  }
		  else if("9月-".equals(data))
		  {
			  return 8;
		  }
		  else if("10月".equals(data))
		  {
			  return 9;
		  }
		  else if("11月".equals(data))
		  {
			  return 10;
		  }
		  else {
			  return 11;
		  }
		//4200214112 雷冰冰
		
	}
	

}
