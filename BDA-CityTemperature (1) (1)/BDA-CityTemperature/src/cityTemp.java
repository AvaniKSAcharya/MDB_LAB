import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class cityTemp {
		
	public static class Mapper_temp extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key, Text values,Context context) throws IOException,InterruptedException
		{
			String line = values.toString();
			String entry[] = line.split(" ");
			
			
			context.write(new Text(entry[2]),new FloatWritable(Float.parseFloat(entry[1])));
		
		}
	}
	public static class Mapper_city extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		public void map(LongWritable key, Text values,Context context) throws IOException,InterruptedException
		{
			String line = values.toString();
			String entry[] = line.split(" ");
			
			
			context.write(new Text(entry[2]),new IntWritable(1));
		
		}
	}
	public static class Mapper_tempB extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key, Text values,Context context) throws IOException,InterruptedException
		{
			String line = values.toString();
			String entry[] = line.split(" ");
			
			if(entry[2].equals("Bangalore")) {
			
			context.write(new Text(entry[2]),new FloatWritable(Float.parseFloat(entry[1])));
			}
		
		}
	}
	
	public static class testReducer_1 extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
		public void reduce(Text key, Iterable <FloatWritable> values,Context context) throws IOException,InterruptedException
		{
			float Max_temp = 0,Min_temp = 999;
			
			for(FloatWritable x:values) {
				
				if(Max_temp < x.get()) {
					Max_temp = x.get();
				}
				if(Min_temp > x.get()) {
					Min_temp = x.get();
				}
								
			}
			context.write(key, new FloatWritable(Max_temp));
			context.write(key, new FloatWritable(Min_temp));
		}
			
	}
	public static class testReducer_2 extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key, Iterable <IntWritable> values,Context context) throws IOException,InterruptedException
		{
			int total_count = 0;
			for(IntWritable x:values) {
				total_count +=x.get();
			}
			context.write(key, new IntWritable(total_count));
		}
			
	}
	

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		//conf.set("Institute", args[5]);
		Job job1 = Job.getInstance(conf,"city-count1");
		job1.setJarByClass(cityTemp.class);
		job1.setMapperClass(Mapper_temp.class);
		job1.setReducerClass(testReducer_1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job1,new Path(args[0]));		
		FileOutputFormat.setOutputPath(job1,new Path(args[1]));
		job1.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2,"city-count2");
		job2.setJarByClass(cityTemp.class);
		job2.setMapperClass(Mapper_city.class);
		job2.setReducerClass(testReducer_2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2,new Path(args[0]));		
		FileOutputFormat.setOutputPath(job2,new Path(args[2]));
		job2.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3,"city-count2");
		job3.setJarByClass(cityTemp.class);
		job3.setMapperClass(Mapper_tempB.class);
		job3.setReducerClass(testReducer_1.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job3,new Path(args[0]));		
		FileOutputFormat.setOutputPath(job3,new Path(args[3]));
		job3.waitForCompletion(true);
		
		System.exit(job2.waitForCompletion(true)?0:1);
	}

}
