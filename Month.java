import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Month
{
	public static class MonthMap extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
		String str[]=values.toString().split(",");
		float amt =Float.parseFloat(str[3]);
		String month = str[1].substring(0,2);
        context.write(new Text(month),new FloatWritable(amt));
		
		
	}
	} 
	
	public static class MonthReduce extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
	public void reduce(Text k,Iterable<FloatWritable> val,Context context) throws IOException,InterruptedException
	{   float sum=0;
		for(FloatWritable f:val)
		{
			sum=sum+f.get();
		}
			
		context.write(k,new FloatWritable (sum));
	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Amount Group by Month");
	
	job.setJarByClass(Month.class);
	job.setMapperClass(MonthMap.class);
	job.setReducerClass(MonthReduce.class);
	
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
  
