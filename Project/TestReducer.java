package main;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

public class TestReducer extends Reducer<IntWritable, Text, Text, Text> {
	
	// getting classifiers
	private Classifier getClassifier(String fileName) throws Exception {
		Path path = new Path("model/" + fileName);
		System.out.println("model/" + fileName);
	    FileSystem fileSystem = path.getFileSystem(new Configuration());
	    FSDataInputStream inputStream = fileSystem.open(path);
		return (Classifier) SerializationHelper.read(inputStream);
	}
	
	/*	
	0			DoubleWritable	Index
	1			BooleanWritable	 Red_Bird
	2			DoubleWritable	 LATITUDE
	3			DoubleWritable	 LONGITUDE
	4			DoubleWritable	 YEAR
	5			DoubleWritable	 MONTH
	6			DoubleWritable	 DAY
	7			DoubleWritable	 TIME
	8			Text	 STATE_PROVINCE
	9			DoubleWritable	 PRIMARY_CHECKLIST_FLAG
	10			DoubleWritable	 ELEV_GT
	11			DoubleWritable	 ELEV_NED
	12			DoubleWritable	 HOUSING_DENSITY
	13			DoubleWritable	 HOUSING_PERCENT_VACANT
	14			DoubleWritable	 DIST_FROM_FLOWING_FRESH
	15			DoubleWritable	 DIST_IN_FLOWING_FRESH
	16			DoubleWritable	 DIST_FROM_STANDING_FRESH
	17			DoubleWritable	 DIST_IN_STANDING_FRESH
	18			DoubleWritable	 DIST_FROM_WET_VEG_FRESH
	19			DoubleWritable	 DIST_IN_WET_VEG_FRESH
	20			DoubleWritable	 DIST_FROM_FLOWING_BRACKISH
	21			DoubleWritable	 DIST_IN_FLOWING_BRACKISH
	22			DoubleWritable	 DIST_FROM_STANDING_BRACKISH
	23			DoubleWritable	 DIST_IN_STANDING_BRACKISH
	24			DoubleWritable	 DIST_FROM_WET_VEG_BRACKISH
	25			DoubleWritable	 DIST_IN_WET_VEG_BRACKISH
	*/
	// builidng instance and adding to set
	private Instance getInstance(String[] sightDetail, Instances set) {
			
		Instance instance = new DenseInstance(25);
		//instance.setValue(set.attribute(0), sightDetail. getRed_Bird().get()+"");
		instance.setValue(set.attribute(1),Double.parseDouble(sightDetail[2]));
		instance.setValue(set.attribute(2), Double.parseDouble(sightDetail[3]));
		instance.setValue(set.attribute(3), Double.parseDouble(sightDetail[4]));
		instance.setValue(set.attribute(4), Double.parseDouble(sightDetail[5]));
		instance.setValue(set.attribute(5), Double.parseDouble(sightDetail[6]));
		instance.setValue(set.attribute(6), Double.parseDouble(sightDetail[7]));
		instance.setValue(set.attribute(7), sightDetail[8].hashCode());	
	
			if(sightDetail[9].equals("1")){
			instance.setValue(set.attribute(8),Double.parseDouble(sightDetail[9]));
			}
			else
				instance.setMissing(set.attribute(8));
			
			if( !sightDetail[10].equals("?")){
				instance.setValue(set.attribute(9), Double.parseDouble(sightDetail[10]));
			}else  
				instance.setMissing(set.attribute(9));
			if( !sightDetail[11].equals("?")){
				instance.setValue(set.attribute(10), Double.parseDouble(sightDetail[11]));
			}else  
				instance.setMissing(set.attribute(10));
			if( !sightDetail[12].equals("?")){
				instance.setValue(set.attribute(11), Double.parseDouble(sightDetail[12]));
			}else  
				instance.setMissing(set.attribute(11));
			if( !sightDetail[13].equals("?")){
				instance.setValue(set.attribute(12), Double.parseDouble(sightDetail[13]));
			}else  
				instance.setMissing(set.attribute(12));
			if( !sightDetail[14].equals("?")){
				instance.setValue(set.attribute(13), Double.parseDouble(sightDetail[14]));
			}else  
				instance.setMissing(set.attribute(13));
			if( !sightDetail[15].equals("?")){
				instance.setValue(set.attribute(14), Double.parseDouble(sightDetail[15]));
			}else  
				instance.setMissing(set.attribute(14));
			if( !sightDetail[16].equals("?")){
				instance.setValue(set.attribute(15), Double.parseDouble(sightDetail[16]));
			}  else  instance.setMissing(set.attribute(15));
			if( !sightDetail[17].equals("?")){
				instance.setValue(set.attribute(16), Double.parseDouble(sightDetail[17]));
			}  else  instance.setMissing(set.attribute(16));
			if(!sightDetail[18].equals("?")){
				instance.setValue(set.attribute(17), Double.parseDouble(sightDetail[18]));
			}  else  instance.setMissing(set.attribute(17));
			if(!sightDetail[19].equals("?")){
				instance.setValue(set.attribute(18), Double.parseDouble(sightDetail[19]));
			}  else  instance.setMissing(set.attribute(18));
			if( !sightDetail[20].equals("?")){
				instance.setValue(set.attribute(19), Double.parseDouble(sightDetail[20]));
			}  else  instance.setMissing(set.attribute(19));
			if( !sightDetail[21].equals("?")){
				instance.setValue(set.attribute(20), Double.parseDouble(sightDetail[21]));
			}  else  instance.setMissing(set.attribute(20));
			if( !sightDetail[22].equals("?")){
				instance.setValue(set.attribute(21), Double.parseDouble(sightDetail[22]));
			}  else  instance.setMissing(set.attribute(21));
			if( !sightDetail[23].equals("?")){
				instance.setValue(set.attribute(22), Double.parseDouble(sightDetail[23]));
			}  else  instance.setMissing(set.attribute(22));
			if( !sightDetail[24].equals("?")){
				instance.setValue(set.attribute(23), Double.parseDouble(sightDetail[24]));
			}  else  instance.setMissing(set.attribute(23));
			if(!sightDetail[25].equals("?")){
				instance.setValue(set.attribute(24),Double.parseDouble(sightDetail[25]));
			}  else  instance.setMissing(set.attribute(24));

		return instance;
	}
	
	// initial instance
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

	int n;    
    ArrayList<Classifier> classifiers = new ArrayList<Classifier>();
    Instances set;


	public void setup(Context context){

		// intializing required vars
		 n = context.getConfiguration().getInt("n", -10);
    	
        if (n == -10) {
            throw new Error("Can not find n");
        }

        set = initSet();

        // reading all the classifiers
        for(int i=0 ; i <n ; i++){
        	try {
			classifiers.add(getClassifier(i+""));
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
		}
	}
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context)  
			throws IOException, InterruptedException {
		
		// predicitng values for all the datasets
		for (Text sightDetail : values){
			// voting vars init
			int t=0;
			int f=0;
			// formating
			String[] sd=sightDetail.toString().split("XXXXXXXXXX");
			
			
			if(sd.length>27)
				System.out.println("SIZE   : "+sd.length);
			else
			{
				Instance instance = getInstance(sd, set);
				instance.setDataset(set);				
				
				for(Classifier classifier : classifiers){
					try {
						double[] fDistribution = classifier.distributionForInstance(instance);
		
						// Voting
						if (fDistribution[0] > fDistribution[1]) {
							t++;
						} else {							
							f++;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// Final write
				if (t > f) {
					context.write(new Text(sd[26] + ",1"), new Text(""));
				} else {
					context.write(new Text(sd[26] + ",0"), new Text(""));
				}
			}
			
		}		
	}
}
