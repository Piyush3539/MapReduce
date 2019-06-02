package com.dataflair.tr.join.adv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class JoinWritable implements Writable
{
	private Text mrValue;
	private Text mrFileName;

	public JoinWritable()
	{
		set(new Text(), new Text());
	}

	public JoinWritable(String mrValue, String mrFileName)
	{
		set(new Text(mrValue), new Text(mrFileName));
	}

	public JoinWritable(Text mrValue, Text mrFileName)
	{
		set(mrValue, mrFileName);
	}

	public void set(Text mrValue, Text mrFileName)
	{
		this.mrValue = mrValue;
		this.mrFileName = mrFileName;
	}

	public Text getMrValue()
	{
		return mrValue;
	}

	public Text getMrFileName()
	{
		return mrFileName;
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		mrValue.write(out);
		mrFileName.write(out);
	}

	public void readFields(DataInput in) throws IOException
	{
		mrValue.readFields(in);
		mrFileName.readFields(in);
	}

	@Override
	public int hashCode()
	{
		return mrValue.hashCode() * 163 + mrFileName.hashCode();
	}
/*
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof MyOutputWritable)
		{
			MyOutputWritable tow = (MyOutputWritable) o;
			return mrValue.equals(tow.mrValue) && mrFileName.equals(tow.mrFileName);
		}
		return false;
	}
*/
	@Override
	public String toString()
	{
		return mrValue + "\t" + mrFileName;
	}
}