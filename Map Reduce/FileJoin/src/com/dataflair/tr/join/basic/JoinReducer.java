package com.dataflair.tr.join.basic;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class JoinReducer extends Reducer <Text, Text, NullWritable, Text>
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		//key = emp-id
		//values = [name, dept]
		
		StringBuffer rec = new StringBuffer(key.toString()).append(",");	
		int count = 0;
		for (Text val : values)
		{
			rec.append(val.toString());
			if (count < 1)
			{
				rec.append(",");
			}
			count++;
		}
		context.write(NullWritable.get(), new Text (rec.toString()));
//		output-val = id, name, dept
	}
}