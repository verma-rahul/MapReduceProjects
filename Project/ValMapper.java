package main;

import java.io.IOException;
import java.util.Arrays;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.Counters;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

public class ValMapper extends Mapper<IntWritable, SightingDetails, Text, Text> {

	// reading a classifier	
	private Classifier getClassifier(String fileName) throws Exception {
		Path path = new Path("model/" + fileName);
		System.out.println("model/" + fileName);
	    FileSystem fileSystem = path.getFileSystem(new Configuration());
	    FSDataInputStream inputStream = fileSystem.open(path);
		return (Classifier) SerializationHelper.read(inputStream);
	}
	
	// creates an instance
	private Instance getInstance(SightingDetails sightDetail, Instances set) {
		Instance instance = new DenseInstance(25);	
		instance.setValue(set.attribute(0), sightDetail. getRed_Bird().get()+"");
		instance.setValue(set.attribute(1), sightDetail. getLATITUDE().get());
		instance.setValue(set.attribute(2), sightDetail. getLONGITUDE().get());
		instance.setValue(set.attribute(3), sightDetail. getYEAR().get());
		instance.setValue(set.attribute(4), sightDetail. getMONTH().get());
		instance.setValue(set.attribute(5), sightDetail. getDAY().get());
		instance.setValue(set.attribute(6), sightDetail. getTIME().get());
		instance.setValue(set.attribute(7), sightDetail. getSTATE_PROVINCE().toString().hashCode());
		instance.setValue(set.attribute(8), sightDetail. getPRIMARY_CHECKLIST_FLAG().get());
				if( sightDetail. getELEV_GT_B().get()){
			instance.setValue(set.attribute(9), sightDetail. getELEV_GT().get());
		}else  
			instance.setMissing(set.attribute(9));
		if( sightDetail. getELEV_NED_B().get()){
			instance.setValue(set.attribute(10), sightDetail. getELEV_NED().get());
		}else  
			instance.setMissing(set.attribute(10));
		if( sightDetail. getHOUSING_DENSITY_B().get()){
			instance.setValue(set.attribute(11), sightDetail. getHOUSING_DENSITY().get());
		}else  
			instance.setMissing(set.attribute(11));
		if( sightDetail. getHOUSING_PERCENT_VACANT_B().get()){
			instance.setValue(set.attribute(12), sightDetail. getHOUSING_PERCENT_VACANT().get());
		}else  
			instance.setMissing(set.attribute(12));
		if( sightDetail. getDIST_FROM_FLOWING_FRESH_B().get()){
			instance.setValue(set.attribute(13), sightDetail. getDIST_FROM_FLOWING_FRESH().get());
		}else  
			instance.setMissing(set.attribute(13));
		if( sightDetail. getDIST_IN_FLOWING_FRESH_B().get()){
			instance.setValue(set.attribute(14), sightDetail. getDIST_IN_FLOWING_FRESH().get());
		}else  
			instance.setMissing(set.attribute(14));
		if( sightDetail. getDIST_FROM_STANDING_FRESH_B().get()){
			instance.setValue(set.attribute(15), sightDetail. getDIST_FROM_STANDING_FRESH().get());
		}  else  instance.setMissing(set.attribute(15));
		if( sightDetail. getDIST_IN_STANDING_FRESH_B().get()){
			instance.setValue(set.attribute(16), sightDetail. getDIST_IN_STANDING_FRESH().get());
		}  else  instance.setMissing(set.attribute(16));
		if( sightDetail. getDIST_FROM_WET_VEG_FRESH_B().get()){
			instance.setValue(set.attribute(17), sightDetail. getDIST_FROM_WET_VEG_FRESH().get());
		}  else  instance.setMissing(set.attribute(17));
		if( sightDetail. getDIST_IN_WET_VEG_FRESH_B().get()){
			instance.setValue(set.attribute(18), sightDetail. getDIST_IN_WET_VEG_FRESH().get());
		}  else  instance.setMissing(set.attribute(18));
		if( sightDetail. getDIST_FROM_FLOWING_BRACKISH_B().get()){
			instance.setValue(set.attribute(19), sightDetail. getDIST_FROM_FLOWING_BRACKISH().get());
		}  else  instance.setMissing(set.attribute(19));
		if( sightDetail. getDIST_IN_FLOWING_BRACKISH_B().get()){
			instance.setValue(set.attribute(20), sightDetail. getDIST_IN_FLOWING_BRACKISH().get());
		}  else  instance.setMissing(set.attribute(20));
		if( sightDetail. getDIST_FROM_STANDING_BRACKISH_B().get()){
			instance.setValue(set.attribute(21), sightDetail. getDIST_FROM_STANDING_BRACKISH().get());
		}  else  instance.setMissing(set.attribute(21));
		if( sightDetail. getDIST_IN_STANDING_BRACKISH_B().get()){
			instance.setValue(set.attribute(22), sightDetail. getDIST_IN_STANDING_BRACKISH().get());
		}  else  instance.setMissing(set.attribute(22));
		if( sightDetail. getDIST_FROM_WET_VEG_BRACKISH_B().get()){
			instance.setValue(set.attribute(23), sightDetail. getDIST_FROM_WET_VEG_BRACKISH().get());
		}  else  instance.setMissing(set.attribute(23));
		if( sightDetail. getDIST_IN_WET_VEG_BRACKISH_B().get()){
			instance.setValue(set.attribute(24), sightDetail. getDIST_IN_WET_VEG_BRACKISH().get());
		}  else  instance.setMissing(set.attribute(24));

		return instance;
	}
	
	// init a set
	private Instances initSet() {
		// Declare numeric attributes
		Attribute date = new Attribute("date");
		Attribute lati = new Attribute("lati");
		Attribute longi = new Attribute("longi");
		Attribute year = new Attribute("year");
		Attribute month = new Attribute("month");
		Attribute day = new Attribute("day");
		Attribute time = new Attribute("time");
		Attribute primary = new Attribute("primary");
		Attribute state = new Attribute("state");
		Attribute elev_gt = new Attribute("elev_gt");
		Attribute elev_nd = new Attribute("elev_nd");
		Attribute house_dense = new Attribute("house_dense");
		Attribute house_vacant = new Attribute("house_vacant");
		Attribute dist_flow_frsh = new Attribute("dist_flow_frsh");
		Attribute dist_in_flow_frsh = new Attribute("dist_in_flow_frsh");
		Attribute dist_stnd_frsh = new Attribute("dist_stnd_frsh");
		Attribute dist_in_stnd_frsh = new Attribute("dist_in_stnd_frsh");
		Attribute dist_wet_veg = new Attribute("dist_wet_veg");
		Attribute dist_in_wet_veg = new Attribute("dist_in_wet_veg");
		Attribute dist_flow_back = new Attribute("dist_flow_back");
		Attribute dist_in_flow_back = new Attribute("dist_in_flow_back");
		Attribute dist_stnd_back = new Attribute("dist_stnd_back");
		Attribute dist_in_stnd_back = new Attribute("dist_in_stnd_back");
		Attribute dist_wet_veg_back = new Attribute("dist_wet_veg_back");
		Attribute dist_in_wet_veg_back = new Attribute("dist_in_wet_veg_back");


		// Declare text attributes converted to hash
		Attribute state_province = new Attribute("state");
		
		// Declare the class attribute along with its values
		List<String> classAttributes = new ArrayList<String>();
		classAttributes.add("true");
		classAttributes.add("false");
		Attribute sight = new Attribute("sight", classAttributes);
		
		// Declare the feature vector
		ArrayList<Attribute> wekaAttributes = new ArrayList<Attribute>();
		wekaAttributes.add(sight);
		wekaAttributes.add(lati);
		wekaAttributes.add(longi);
		wekaAttributes.add(year);
		wekaAttributes.add(month);
		wekaAttributes.add(day);
		wekaAttributes.add(time);
		wekaAttributes.add(state);
		wekaAttributes.add(primary);
		wekaAttributes.add(elev_gt);
		wekaAttributes.add(elev_nd);
		wekaAttributes.add(house_dense);
		wekaAttributes.add(house_vacant);
		wekaAttributes.add(dist_flow_frsh);
		wekaAttributes.add(dist_in_flow_frsh);
		wekaAttributes.add(dist_stnd_frsh);
		wekaAttributes.add(dist_in_stnd_frsh);
		wekaAttributes.add(dist_wet_veg);
		wekaAttributes.add(dist_in_wet_veg);
		wekaAttributes.add(dist_flow_back);
		wekaAttributes.add(dist_in_flow_back);
		wekaAttributes.add(dist_stnd_back);
		wekaAttributes.add(dist_in_stnd_back);
		wekaAttributes.add(dist_wet_veg_back);
		wekaAttributes.add(dist_in_wet_veg_back);
		
		
		// Create an empty training set
		Instances set = new Instances("Model", wekaAttributes, 0);
		// Set class index
		set.setClassIndex(0);
		
		return set;
	}

	// vars 

	int correct;
	int incorrect;
	int n;
    public static final String VAL_COUNTER_GROUP = "correct";
    ArrayList<Classifier> classifiers = new ArrayList<Classifier>();
    Instances set;


	public void setup(Context context){
		//init vars
		 correct = 0;
		 incorrect = 0;
		 n = context.getConfiguration().getInt("n", -10);
    	
        if (n == -10) {
            throw new Error("Can not find n");
        }

        set = initSet();
        // read all classifiers
        for(int i=0 ; i <n ; i++){
        	try {
			classifiers.add(getClassifier(i+""));
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
	}

	@Override
	public void map(IntWritable key, SightingDetails value, Context context) 
		throws IOException, InterruptedException {
		// init voting vars
		int t=0;
		int f=0;
		String format="";
		Instance instance = getInstance(value, set);
		instance.setDataset(set);
		for(Classifier classifier : classifiers){
			try {
				// voting
				double[] fDistribution = classifier.distributionForInstance(instance);
				format = value.getSampleId().toString();
				if (fDistribution[0] > fDistribution[1]) {
					t++;
				} else {
					f++;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// final write
		if (t > f) {
			context.write(new Text(format + ",1"), new Text(""));
			//calc accuracy
			if(value.getRed_Bird().get())
				correct ++;
			else
				incorrect++;
		} else {
			context.write(new Text(format + ",0"), new Text(""));
			//calc accuracy
			if(!value.getRed_Bird().get())
				correct ++;
			else
				incorrect ++;
		}
	}

	public void cleanup(Context context){
		//for accuracy
		context.getCounter(VAL_COUNTER_GROUP,"correct").increment(correct);
		context.getCounter(VAL_COUNTER_GROUP,"incorrect").increment(incorrect);
	} 
}
