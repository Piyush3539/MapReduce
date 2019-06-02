package com.dataflair.hd.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class IntSumReducer extends Reducer <Text, IntWritable, Text, IntWritable>
{
	private IntWritable result = new IntWritable();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
//		Hi	[1 1 1 1 1 1....]
		
		int sum = 0;
		for (IntWritable val : values)
		{
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
