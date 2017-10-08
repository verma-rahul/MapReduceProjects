package main;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;


import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.meta.ClassificationViaRegression;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.pmml.consumer.SupportVectorMachineModel;
import weka.classifiers.trees.RandomTree;

import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.DenseInstance;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SerializationHelper;

import java.io.ObjectOutputStream;

public class CheckReducer extends Reducer<IntWritable, SightingDetails,Text, BytesWritable>{

	
	private Instances initTrainingSet() {
		
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
		Instances trainingSet = new Instances("Model", wekaAttributes, 0);
		// Set class index
		trainingSet.setClassIndex(0);
		
		return trainingSet;
		
	}
	
	private void addInstance(SightingDetails sightDetail, Instances trainingSet) {

		Instance instance = new DenseInstance(25);	
		instance.setValue(trainingSet.attribute(0), sightDetail. getRed_Bird().get()+"");
		instance.setValue(trainingSet.attribute(1), sightDetail. getLATITUDE().get());
		instance.setValue(trainingSet.attribute(2), sightDetail. getLONGITUDE().get());
		instance.setValue(trainingSet.attribute(3), sightDetail. getYEAR().get());
		instance.setValue(trainingSet.attribute(4), sightDetail. getMONTH().get());
		instance.setValue(trainingSet.attribute(5), sightDetail. getDAY().get());
		instance.setValue(trainingSet.attribute(6), sightDetail. getTIME().get());
		instance.setValue(trainingSet.attribute(7), sightDetail. getSTATE_PROVINCE().toString().hashCode());
		instance.setValue(trainingSet.attribute(8), sightDetail. getPRIMARY_CHECKLIST_FLAG().get());
				if( sightDetail. getELEV_GT_B().get()){
			instance.setValue(trainingSet.attribute(9), sightDetail. getELEV_GT().get());
		}else  
			instance.setMissing(trainingSet.attribute(9));
		if( sightDetail. getELEV_NED_B().get()){
			instance.setValue(trainingSet.attribute(10), sightDetail. getELEV_NED().get());
		}else  
			instance.setMissing(trainingSet.attribute(10));
		if( sightDetail. getHOUSING_DENSITY_B().get()){
			instance.setValue(trainingSet.attribute(11), sightDetail. getHOUSING_DENSITY().get());
		}else  
			instance.setMissing(trainingSet.attribute(11));
		if( sightDetail. getHOUSING_PERCENT_VACANT_B().get()){
			instance.setValue(trainingSet.attribute(12), sightDetail. getHOUSING_PERCENT_VACANT().get());
		}else  
			instance.setMissing(trainingSet.attribute(12));
		if( sightDetail. getDIST_FROM_FLOWING_FRESH_B().get()){
			instance.setValue(trainingSet.attribute(13), sightDetail. getDIST_FROM_FLOWING_FRESH().get());
		}else  
			instance.setMissing(trainingSet.attribute(13));
		if( sightDetail. getDIST_IN_FLOWING_FRESH_B().get()){
			instance.setValue(trainingSet.attribute(14), sightDetail. getDIST_IN_FLOWING_FRESH().get());
		}else  
			instance.setMissing(trainingSet.attribute(14));
		if( sightDetail. getDIST_FROM_STANDING_FRESH_B().get()){
			instance.setValue(trainingSet.attribute(15), sightDetail. getDIST_FROM_STANDING_FRESH().get());
		}  else  instance.setMissing(trainingSet.attribute(15));
		if( sightDetail. getDIST_IN_STANDING_FRESH_B().get()){
			instance.setValue(trainingSet.attribute(16), sightDetail. getDIST_IN_STANDING_FRESH().get());
		}  else  instance.setMissing(trainingSet.attribute(16));
		if( sightDetail. getDIST_FROM_WET_VEG_FRESH_B().get()){
			instance.setValue(trainingSet.attribute(17), sightDetail. getDIST_FROM_WET_VEG_FRESH().get());
		}  else  instance.setMissing(trainingSet.attribute(17));
		if( sightDetail. getDIST_IN_WET_VEG_FRESH_B().get()){
			instance.setValue(trainingSet.attribute(18), sightDetail. getDIST_IN_WET_VEG_FRESH().get());
		}  else  instance.setMissing(trainingSet.attribute(18));
		if( sightDetail. getDIST_FROM_FLOWING_BRACKISH_B().get()){
			instance.setValue(trainingSet.attribute(19), sightDetail. getDIST_FROM_FLOWING_BRACKISH().get());
		}  else  instance.setMissing(trainingSet.attribute(19));
		if( sightDetail. getDIST_IN_FLOWING_BRACKISH_B().get()){
			instance.setValue(trainingSet.attribute(20), sightDetail. getDIST_IN_FLOWING_BRACKISH().get());
		}  else  instance.setMissing(trainingSet.attribute(20));
		if( sightDetail. getDIST_FROM_STANDING_BRACKISH_B().get()){
			instance.setValue(trainingSet.attribute(21), sightDetail. getDIST_FROM_STANDING_BRACKISH().get());
		}  else  instance.setMissing(trainingSet.attribute(21));
		if( sightDetail. getDIST_IN_STANDING_BRACKISH_B().get()){
			instance.setValue(trainingSet.attribute(22), sightDetail. getDIST_IN_STANDING_BRACKISH().get());
		}  else  instance.setMissing(trainingSet.attribute(22));
		if( sightDetail. getDIST_FROM_WET_VEG_BRACKISH_B().get()){
			instance.setValue(trainingSet.attribute(23), sightDetail. getDIST_FROM_WET_VEG_BRACKISH().get());
		}  else  instance.setMissing(trainingSet.attribute(23));
		if( sightDetail. getDIST_IN_WET_VEG_BRACKISH_B().get()){
			instance.setValue(trainingSet.attribute(24), sightDetail. getDIST_IN_WET_VEG_BRACKISH().get());
		}  else  instance.setMissing(trainingSet.attribute(24));

		trainingSet.add(instance);
	}
	

	private Classifier classify(Instances trainingSet) throws Exception {
		// build classifier with tuned parameters 
		RandomTree rt = new RandomTree();
		rt.setMaxDepth(0);
		rt.setKValue(5);
		rt.setMinNum(1);
		rt.buildClassifier(trainingSet);
		return rt;
	}
	
	//Instances trainingSet;
	String model_path;

	public void setup(Context context){

			//trainingSet = initTrainingSet();
		model_path = context.getConfiguration().getStrings("model", "")[0];
		if (model_path.equals("")) {
            throw new Error("Can not find model path");
        }
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<SightingDetails> values, Context context)  
			throws IOException, InterruptedException {
		int count=0;

		Instances trainingSet = initTrainingSet();
		
		for (SightingDetails sightingDetail : values) {
			addInstance(sightingDetail, trainingSet);
			count++;
		}

		System.out.println("Training = > " + key.get() + "has size =" + count);
		
		try {
			//wrtiting the classifier on disk
			Classifier model = classify(trainingSet);
			Path pt = new Path(model_path+"/"+key.get()+"");
            FileSystem fs = pt.getFileSystem(context.getConfiguration());
            FSDataOutputStream fdo = fs.create(pt);
            ObjectOutputStream out = new ObjectOutputStream(fdo);
            out.writeObject(model);
            out.flush();
            out.close();
            fdo.close();
            fs.close();
			FileSystem fileSystem = FileSystem.get(URI.create("model/"),
					new Configuration());
			FSDataOutputStream fsDataOutputStream = fileSystem.create(
					new Path("model/" + key.toString()));
			
			weka.core.SerializationHelper.write(fsDataOutputStream, model);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}