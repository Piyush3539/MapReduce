package com.dataflair.hd;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpInfoMapper extends Mapper<Object, Text, Text, Text>
{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
//		emp-id	Name	Age	Gender	Dept	CTC
//		101		Alice	23	female	IT		45

		String[] tokens = value.toString().split("\t");
		String dept = tokens[4].toString();

		context.write(new Text(dept), value);
	}
}
