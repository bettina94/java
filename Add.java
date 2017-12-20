import java.io.*;
//import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Add
{
	public static class TranMap extends Mapper<LongWritable,Text,Text,Text>
	{
	public void map(LongWritable keys,Text values,Context context) throws IOException,InterruptedException
	{
	String str[]=values.toString().split(",");
	
	String userid=str[2];
	String tran_amt=str[0]+","+str[3];
	context.write(new Text(userid),new Text(tran_amt));
	}
	} 
	
	public static class TranReduce extends Reducer<Text,Text,IntWritable,FloatWritable>
	{
	public void reduce(Text k,Iterable<Text> val,Context context) throws IOException,InterruptedException
	{   float sum=0;
	    int count=0;
		for(Text t:val)
		{
		String ttr[]=t.toString().split(",");
		//String tran = ttr[0];
	    count++;
		float amt = Float.parseFloat(ttr[1]);
		sum=sum+amt;
		}
		
		context.write(new IntWritable(count),new FloatWritable(sum));

	}
	}
	
	public static void main(String args[]) throws Exception
	{
	Configuration conf=new Configuration();
	Job job=Job.getInstance(conf,"Transaction");
	
	job.setJarByClass(Add.class);
	job.setMapperClass(TranMap.class);
	job.setReducerClass(TranReduce.class);
	
	job.setNumReduceTasks(1);
	
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(FloatWritable.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job,new Path (args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	
	System.exit(job.waitForCompletion(true)? 0 : 1);
	}
	
	
	
	
	
	}
  
