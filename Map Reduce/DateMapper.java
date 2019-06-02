package org.apache.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public final class DateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>
{	DoubleWritable d= new DoubleWritable();
	private final Text category = new Text();
	
	public final void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException
	{
		final String line = value.toString();	

		final String[] data = line.trim().split("\t");
		//data[0]= 2012-01-01
				//data[1]= 09:00
				//data[2]= San Jose
				//data[3]= Men's Clothing
				//data[4]= 214.05
				//data[5]= Amex
		if (data.length == 6)
		{
			final String date = data[0];
			final double sales = Double.parseDouble(data[4]);
			category.set(date);
			d.set(sales);
			context.write(category, d);
		}
	}
}