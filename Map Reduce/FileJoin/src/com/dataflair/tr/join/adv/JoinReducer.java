package com.dataflair.tr.join.adv;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer <Text, JoinWritable, NullWritable, Text>
{
	public void reduce(Text key, Iterable<JoinWritable> values, Context context) throws IOException, InterruptedException
	{
		String name = null;
		String dept = null;
		int count = 0;
		for (JoinWritable mow : values)
		{
			if (mow.getMrFileName().toString().equals("empname.txt"))
			{
				name = mow.getMrValue().toString();
			}
			else if (mow.getMrFileName().toString().equals("empdept.txt"))
			{
				dept = mow.getMrValue().toString();
			}
		}
		StringBuffer rec = new StringBuffer(key.toString()).append(",");
		rec.append(name).append(",").append(dept);
		
		context.write(NullWritable.get(), new Text (rec.toString()));
	}
}
