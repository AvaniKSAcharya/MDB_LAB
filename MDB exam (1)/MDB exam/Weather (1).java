import java.io.IOException;
import java.util.Iterator;

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


public class Weather {

	public static class testMapper extends Mapper<LongWritable,Text, Text, FloatWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
            String line = values.toString();
            String[] list = line.split(" ");
            
            
            context.write(new Text(list[2]),new FloatWritable(Float.parseFloat(list[1])));
   
    }}
	public static class testMapper_1 extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		public void map(LongWritable key, Text values,Context context) throws IOException,InterruptedException
		{
			String line = values.toString();
			String entry[] = line.split(" ");
			
			
			context.write(new Text(entry[2]),new IntWritable(1));
		
		}
	}
	
	public static class testMapper_2 extends Mapper<LongWritable,Text, Text, FloatWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
           String line = values.toString();
           String[] list = line.split(" ");
           
           if(list[2].equals("Bangalore"))
           {
           
        	   context.write(new Text(list[2]),new FloatWritable(Float.parseFloat(list[1])));
    }}}
	
	public static class testMapper_3 extends Mapper<LongWritable,Text, Text, FloatWritable> {
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException{
        	String par = context.getConfiguration().get("CITY");
        	String line = values.toString();
            String[] city = line.split(" ");
            if(city[2].equals(par))
            {
            context.write(new Text(city[2]),new FloatWritable(Float.parseFloat(city[1])));
            }   
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
	
	public static class testReduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
        public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
        	float Max = 0,Min = 999;
        	
        	
			for(FloatWritable x: values) {
				if(Max < x.get())
				{
					Max = x.get();
				}
				if(Min > x.get())
				{
					Min = x.get();
				}
			}
			context.write(key, new FloatWritable(Max));
			context.write(key, new FloatWritable(Min));
        }
        
    }
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
				Configuration conf = new Configuration();
				conf.set("CITY", args[5]);
				Job job1 = Job.getInstance(conf,"city-count1");
				job1.setJarByClass(Weather.class);
				job1.setMapperClass(testMapper.class);
				job1.setReducerClass(testReduce.class);
				job1.setOutputKeyClass(Text.class);
				job1.setOutputValueClass(FloatWritable.class);
				
				FileInputFormat.addInputPath(job1,new Path(args[0]));		
				FileOutputFormat.setOutputPath(job1,new Path(args[1]));
				job1.waitForCompletion(true);
				
				Configuration conf2 = new Configuration();
				Job job2 = Job.getInstance(conf2,"city-count2");
				job2.setJarByClass(Weather.class);
				job2.setMapperClass(testMapper_1.class);
				job2.setReducerClass(testReducer_2.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(IntWritable.class);
				
				FileInputFormat.addInputPath(job2,new Path(args[0]));		
				FileOutputFormat.setOutputPath(job2,new Path(args[2]));
				job2.waitForCompletion(true);
				
				Configuration conf3 = new Configuration();
				Job job3 = Job.getInstance(conf3,"city-count2");
				job3.setJarByClass(Weather.class);
				job3.setMapperClass(testMapper_2.class);
				job3.setReducerClass(testReduce.class);
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(FloatWritable.class);
				
				FileInputFormat.addInputPath(job3,new Path(args[0]));		
				FileOutputFormat.setOutputPath(job3,new Path(args[3]));
				job3.waitForCompletion(true);
				
				Configuration conf4 = new Configuration();
				Job job4 = Job.getInstance(conf3,"city-count2");
				job4.setJarByClass(Weather.class);
				job4.setMapperClass(testMapper_2.class);
				job4.setReducerClass(testReduce.class);
				job4.setOutputKeyClass(Text.class);
				job4.setOutputValueClass(FloatWritable.class);
				
				FileInputFormat.addInputPath(job4,new Path(args[0]));		
				FileOutputFormat.setOutputPath(job4,new Path(args[4]));
				job4.waitForCompletion(true);
				
				System.exit(job4.waitForCompletion(true)?0:1);
	}
}
