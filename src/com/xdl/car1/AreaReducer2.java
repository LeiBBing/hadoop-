package com.xdl.car1;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AreaReducer2 extends Reducer<Text,IntWritable,Text,Text>{
public void reduce(Text key,
java.lang.Iterable<IntWritable> values,org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,Text>.Context context) throws java.io.IOException,InterruptedException{
int count=0;
for(IntWritable val:values) {
count+=val.get();
}
context.write(key,new Text(""+count));
};
//4200214112 雷冰冰
};