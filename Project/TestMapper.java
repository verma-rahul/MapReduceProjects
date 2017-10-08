package main;


import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TestMapper extends Mapper<Object, Text, IntWritable, Text> {

	@Override
	public void map(Object key, Text value, Context context) 
		throws IOException, InterruptedException {
		
	
		Random rand=new Random();
		String[] columns = value.toString().split(",");					
		// excluding headers  
		if(!columns[0].startsWith("Samp")&&!columns[2].contains("LATITUDE")&&!columns[0].isEmpty()){
			// formmating  
			String result = "";
			if(columns[26].equals("?") || columns[26].equals("0")) 
				result="false"; 
			else 
				result="true";
			// converting
			String[] filtered={columns[1657] ,result , columns[2],columns[3],columns[4],columns[5],columns[6],columns[7],columns[10],columns[18],columns[958],columns[959],columns[956]							,columns[957],columns[1090],columns[1091],columns[1092],columns[1093],columns[1094],columns[1095],columns[1096],columns[1097],columns[1098],columns[1099],columns[1100],columns[1101],columns[0]};			
			String concat=columns[1657] +"XXXXXXXXXX"+result +"XXXXXXXXXX"+columns[2]+"XXXXXXXXXX"+columns[3]+"XXXXXXXXXX"+columns[4]+"XXXXXXXXXX"+columns[5]+"XXXXXXXXXX"+					columns[6]+"XXXXXXXXXX"+columns[7]+"XXXXXXXXXX"+columns[10]+"XXXXXXXXXX"+columns[18]+"XXXXXXXXXX"+columns[958]+"XXXXXXXXXX"+columns[959]+"XXXXXXXXXX"+columns[956]+"XXXXXXXXXX"+columns[957]+"XXXXXXXXXX"+columns[1090]+"XXXXXXXXXX"+columns[1091]+"XXXXXXXXXX"+columns[1092]+"XXXXXXXXXX"+columns[1093]									+"XXXXXXXXXX"+columns[1094]+"XXXXXXXXXX"+columns[1095]+"XXXXXXXXXX"+columns[1096]+"XXXXXXXXXX"+columns[1097]+"XXXXXXXXXX"+columns[1098]+"XXXXXXXXXX"+columns[1099]+"XXXXXXXXXX"+columns[1100]+"XXXXXXXXXX"+columns[1101]+"XXXXXXXXXX"+columns[0];
			
			context.write(new IntWritable(Integer.parseInt(filtered[0])), new Text(concat));
		}
		else{
		  
		}
	}
}
