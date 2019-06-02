package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class CityMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{	DoubleWritable d= new DoubleWritable();
	private final Text category = new Text();
	
	public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String line = value.toString();	

		final String[] data = line.trim().split("\t");
		
		if (data.length == 6)
		{
			final String city = data[2];
			final double sales = Double.parseDouble(data[4]);
			category.set(city);
			d.set(sales);
			context.write(category, d);
		}
	}
}