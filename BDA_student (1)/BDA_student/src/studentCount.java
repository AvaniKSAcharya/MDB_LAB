import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class studentCount {
	
	public static class Mapper_Inst extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
		{
			String line = value.toString();
			//String Institute = line.substring(0,5);
			String student[] = line.split(" ");
			String param = context.getConfiguration().get("Institute");
			
			if(student[1].equals(param)){
				System.out.println("testin");
				context.write(new Text(student[1]), new IntWritable(1));

			}		
			
		}
	}
	public static class Mapper_Prog extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
		{
			String line = value.toString();
			String student[] = line.split(" ");
			String param = context.getConfiguration().get("Program");
			
			if(student[2].equals(param)){
				context.write(new Text(student[2]), new IntWritable(1));

			}		
			
		}
	}
	public static class Mapper_Gen extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
		{
			String line = value.toString();
			String student[] = line.split(" ");
			String param = context.getConfiguration().get("Gender");
			String gen = new String("Gender");
			
			if(gen.equals(param) ){
				if(student[3].equals("Boy") || student[3].equals("Girl")) {
					context.write(new Text(student[3]), new IntWritable(1));		
				}
			}					
		}
	}
	public static class Mapper_InstGen extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
		{
			String line = value.toString();
			String student[] = line.split(" ");
			String param = context.getConfiguration().get("Insti_Gen");
			
			if(student[1].equals(param)){
				if(student[3].equals("Boy") || student[3].equals("Girl")) {
					context.write(new Text(student[3]), new IntWritable(1));		
				}
			}					
		}
	}
	public static class testReducer extends Reducer<Text,IntWritable,Text,IntWritable>
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
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("Institute", args[5]);
		Job job1 = Job.getInstance(conf,"student-count1");
		job1.setJarByClass(studentCount.class);
		job1.setMapperClass(Mapper_Inst.class);
		job1.setReducerClass(testReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job1,new Path(args[0]));		
		FileOutputFormat.setOutputPath(job1,new Path(args[1]));
		job1.waitForCompletion(true);
		
		Configuration conf1 = new Configuration();
		conf1.set("Program", args[5]);
		Job job2 = Job.getInstance(conf1,"student-count2");
		job2.setJarByClass(studentCount.class);
		job2.setMapperClass(Mapper_Prog.class);
		job2.setReducerClass(testReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job2,new Path(args[0]));		
		FileOutputFormat.setOutputPath(job2,new Path(args[2]));
		job2.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		conf2.set("Gender", args[5]);
		Job job3 = Job.getInstance(conf2,"student-count3");
		job3.setJarByClass(studentCount.class);
		job3.setMapperClass(Mapper_Gen.class);
		job3.setReducerClass(testReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job3,new Path(args[0]));
		FileOutputFormat.setOutputPath(job3,new Path(args[3]));
		job3.waitForCompletion(true);
		
		Configuration conf3 = new Configuration();
		conf3.set("Insti_Gen", args[5]);
		Job job4 = Job.getInstance(conf3,"student-count4");
		job4.setJarByClass(studentCount.class);
		job4.setMapperClass(Mapper_InstGen.class);
		job4.setReducerClass(testReducer.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job4,new Path(args[0]));
		FileOutputFormat.setOutputPath(job4,new Path(args[4]));
		job4.waitForCompletion(true);
		System.exit(job4.waitForCompletion(true)?0:1);

	}

}
