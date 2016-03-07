package com.rene.app;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



/**
 * Hello world!
 *
 */
public class App
{

	
  public static void main(String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJarByClass(App.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(frequencyMapper.class);
    job.setReducerClass(frequencyReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.waitForCompletion(true);
  }
}


class frequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString()
				.replaceAll("[^A-Za-z]+", "")
				.replaceAll("\\W", "")
				.toLowerCase()
				.split("(?!x`^)");
		
		String substring = ""; 
		for (String s : tokens) {
			substring += s;
			
			if(substring.length() == 2){
				context.write(new Text(substring), new IntWritable(1));
				substring = "" + substring.charAt(1);
			}	
		}
	} 
}
	



class frequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	HashMap<Character, HashMap<Character, Integer>> frequency = new HashMap<Character, HashMap<Character, Integer>>();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {		
		String line =  key.toString();
		int sum = 0;
		for(IntWritable value : values){
			sum += value.get();
		}
		
		HashMap<Character, Integer>tempMap = new HashMap<Character, Integer>();
		
		if(frequency.get(line.charAt(0)) == null){
			tempMap.put(line.charAt(1), sum);
		}else{
			tempMap = frequency.get(line.charAt(0));
			tempMap.put(line.charAt(1), sum);
		}
		frequency.put(line.charAt(0), tempMap);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
    {
		this.print();
	}
	
	
	
	private void print(){
		for(char x: frequency.keySet()){
			for(char y : frequency.get(x).keySet()){
				System.out.print("\t"+y);
			}
			System.out.println();
			break;
		}
		for (char x :frequency.keySet()){
			System.out.print(x + "\t");
			for(char y : frequency.get(x).keySet()){
		
				System.out.print(frequency.get(x).get(y) + "\t");
			}
			System.out.println();
		}
	}
}
