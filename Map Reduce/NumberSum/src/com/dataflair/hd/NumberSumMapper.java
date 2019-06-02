package com.dataflair.hd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NumberSumMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
//		34 34 34 3 4 3 23 2 6 5 67
		StringTokenizer itr = new StringTokenizer(value.toString());
		int sum = 0;
		while (itr.hasMoreTokens())
		{
			int no = Integer.parseInt(itr.nextToken());
			sum += no;
		}
		context.write(new Text ("Sum"), new IntWritable(sum));
	}//a[2]
}