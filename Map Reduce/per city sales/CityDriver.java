package org.apache.hadoop.examples;
import java.io.*;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class CityDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // TODO Auto-generated method stub
            Configuration conf=new Configuration();
            String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
            if(otherArgs.length!=2)
            {
                System.err.println("Error");
                System.exit(2);
            }
            Job job=new Job(conf, "sum");
            job.setJarByClass(CityDriver.class);
            job.setMapperClass(CityMapper.class);
            job.setReducerClass(CityReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
            System.exit(job.waitForCompletion(true)?0:1);
    }

}