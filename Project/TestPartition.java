package main;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TestPartition extends Partitioner<IntWritable, Text> implements Configurable {

	private static int divideby;	
	Configuration conf;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
		int size= conf.getInt("unlabeledSize", -10);
		
		int division=conf.getInt("n", -10);
		
		divideby=size/division+1;
		
		System.out.println("size of on tenth  "+divideby+"size :  "+size);
		 if ((size == -10)||(division==-10)) {
			
	            throw new Error("Can not find n or size");	            
	           
	        }

	}
	
	@Override
	public Configuration getConf() {
		return this.conf;
	}
	
	
	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {		
	  return key.get()/divideby;
	}
}
