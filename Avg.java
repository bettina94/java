import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Avg
{
	public static class AvgMap extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split(",");
	    String userid=str[2];
	    String amt=str[3];
	    float f=Float.parseFloat(amt);
	    context.write(new Text(userid),new FloatWritable(f));
	}
	} 
	
	public static class AvgReduce extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
	public void reduce(Text k,Iterable<FloatWritable> val,Context context) throws IOException,InterruptedException
	{
		int count=0;
		float avg=0;
		float sum=0;
		for(FloatWritable ff:val)
		{
			count++;
			sum=sum+ff.get();
			
				
			avg=sum/count;	
			
			
			
		}
		context.write(k,new FloatWritable(avg));
	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Transaction");
	
	job.setJarByClass(Avg.class);
	job.setMapperClass(AvgMap.class);
	job.setReducerClass(AvgReduce.class);
	
	job.setNumReduceTasks(0);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
	
	
	
	
	}
  
