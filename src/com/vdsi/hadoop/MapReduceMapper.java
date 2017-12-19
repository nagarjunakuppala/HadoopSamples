package com.vdsi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapReduceMapper extends Mapper<Object, Text, Text, IntWritable>{

	private Text words = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		StringTokenizer st = new StringTokenizer(value.toString());
		while(st.hasMoreTokens()){
			words.set(st.nextToken());
			System.out.println("word is:" +words);
			context.write(words, new IntWritable(1));
		}
	}
	
}
