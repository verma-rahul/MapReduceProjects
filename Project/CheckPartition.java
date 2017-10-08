package main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CheckPartition extends Partitioner<IntWritable, SightingDetails> {

	@Override
	public int getPartition(IntWritable key, SightingDetails value, int numPartitions) {
	  // to divide the load of building models equally amongst machines , considering number of machines > number of trees to be trained
	  return Math.abs(key.get() % numPartitions);
	}
}
