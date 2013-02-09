package MapReduce;

import java.io.*;
import java.math.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MapReduce
{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text gpa = new Text();
		private Text word = new Text();
		  
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			int age=0;
			int sex=0;
			int program=0;
			for(int i=0;itr.hasMoreTokens();i++)
			{
				String s = itr.nextToken();
				switch(i)
				{
					case 0: age=Integer.parseInt(s);
					break;
					case 1: sex=Integer.parseInt(s);
					break;
					case 2: program=Integer.parseInt(s);
					break;
					default: 
						gpa.set(s);
						break;
				}
			}
			if(age>=15 & age<=20)
			{
				word.set("G1");
			}
			else if(age>=21 & age<=25)
			{
				word.set("G2");
			}
			else if(age>=26 & age<=30)
			{
				word.set("G3");
			}
			else if(age>30)
			{
				word.set("G4");
			}	

			context.write(word,gpa);

			if(sex==1)
			{
				word.set("M");
			}
			else if(sex==2)
			{
				word.set("F");
			}
			
			context.write(word,gpa);
	
			if(program==1)
			{
				word.set("BS");
			}
			else if(program==2)
			{
				word.set("MS");
			}
			else if(program==3)
			{
				word.set("PHD");
			}

			 context.write(word,gpa);
		}
	}


	
	public static class Red extends Reducer<Text,Text,Text,Text> 
	{
		private Text word = new Text();
    		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			int noofrecords = 0;
			double max=0.0;
			double min=4.0;
			double avg=0.0;
			double sum=0.0;
			double stp1=0.0;
			double stp2=0.0;
			double sdev=0.0;
			for (Text value : values)
				{
                                        double gp=Double.parseDouble(value.toString());
                                        noofrecords = noofrecords+1;
                                        sum=sum+gp;
                                        if( max<gp)
                                        {
                                                max=gp;
                                        }
                                        if(min>gp)
                                        {
                                                min=gp;
                                        }
                                        avg=sum/(double)noofrecords;
                                        stp1=stp1+((gp-avg)*(gp-avg));
                                }
			stp2=stp1/(double)(noofrecords-1);
			sdev=Math.sqrt(stp2);
			word.set("noofrecords="+noofrecords+"  "+"Average GPA="+avg+"  "+"MAX GPA="+max+"  "+"MIN GPA="+min+"  "+"Standard Deviation="+sdev);
			key.set(key.toString() +" ");
			context.write(key, word);

		}
	}
	
	
	public static class MapTwo extends Mapper<Object,Text,Text,Text>
	{
		private Text word=new Text();
		private Text temp=new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException
		{
			String Line=value.toString();
			int index1=Line.indexOf("noofrecords");
			String str1=Line.substring(0,index1);
			String str2=Line.substring(index1);
			String line1="\n"+str1.trim()+":  "+str2;
			temp.set(line1);
				
			if(str1.trim().equals("G1")||str1.trim().equals("G2")||str1.trim().equals("G3")||str1.trim().equals("G4"))
				word.set("Statistics based on Age: ");
			if(str1.trim().equals("M")||str1.trim().equals("F"))
				word.set("Statistics based on Gender: ");
			if(str1.trim().equals("BS")||str1.trim().equals("MS")||str1.trim().equals("PHD"))
				word.set("Statistics based on Program: ");
			context.write(word,temp);
		}
	}
	
	
	
	public static class RedTwo extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			Text word = new Text();
                       	String temp= "";
			for (Text value : values)
              	       	{
                		temp = value.toString()+temp;
                        }
                       	word.set(temp);
                       	context.write(key,word);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MapReduce");
		job.setJarByClass(MapReduce.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(Red.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"_1"));
		Job job2 = new Job(conf,"MapReduce");
		job2.setJarByClass(MapReduce.class);
		job2.setMapOutputKeyClass(Text.class);
		job.waitForCompletion(true);            
        job2.setMapOutputValueClass(Text.class);
        job2.setMapperClass(MapTwo.class);
        job2.setCombinerClass(RedTwo.class);
        job2.setReducerClass(RedTwo.class);
		FileInputFormat.setInputPaths(job2, args[1]+"_1");
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);		
		
	}

}
