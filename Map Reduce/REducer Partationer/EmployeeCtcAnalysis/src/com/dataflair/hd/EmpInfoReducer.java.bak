package com.dataflair.hd;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpInfoReducer extends Reducer <Text, Text, NullWritable, Text>
{
	@Override
	public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	{
		int maxCtc = Integer.MIN_VALUE;
		int ctc = 0;
		String value = "";

		for(Text val: values)
		{
			String [] valTokens = val.toString().split("\\t");
			ctc = Integer.parseInt(valTokens[5]);

			if(ctc > maxCtc)
			{
				value = val.toString();
				maxCtc = ctc;
			}
		}
		context.write(NullWritable.get(), new Text(value));
	}
}
