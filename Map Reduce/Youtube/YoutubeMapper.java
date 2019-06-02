import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class YoutubeMapper extends Mapper <LongWritable, Text, Text, IntWritable>
{
	
	private Text word = new Text();
	
	IntWritable one= new IntWritable();
	
	

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
	String d=value.toString(); 

	
	String[] data=d.trim().split("\t");
	
	
	if(data.length==10)
		
	{
		String category=data[3];
		
		word.set(category);
		
		context.write(word,one);
	
	
				
	}
	
	}
	
}
	
	


