package com.mlrit;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Wordcount{

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String word;
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word = tokenizer.nextToken();
				context.write(new Text(word), one);
			}
		}

	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text word, Iterable<IntWritable> list, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> iterator = list.iterator();
			while (iterator.hasNext()) {
				sum += iterator.next().get();
				
			}
			context.write(word, new IntWritable(sum));
		}

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		try {
			Job job = new Job(new Configuration(),"WordCount");
			job.setJarByClass(Wordcount.class);
			job.setInputFormatClass(TextInputFormat.class);
			
			job.setMapperClass(WordCountMapper.class);
			job.setCombinerClass(WordCountReducer.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(2);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
		} catch (Exception e) {
			
			e.printStackTrace();
		} 
	}

}
