package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DateReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable>
{
	private DoubleWritable result = new DoubleWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException
	{
		
		double sum = 0;
		for (DoubleWritable val : values)
		{
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
