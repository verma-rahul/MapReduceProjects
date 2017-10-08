package main;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class SightingDetails implements Writable {
	
	// all required attributes for the training set
	private	IntWritable	 Index = null	;
	private	BooleanWritable	 Red_Bird = null		;
	private	DoubleWritable	 LATITUDE = null	;
	private	DoubleWritable	 LONGITUDE = null	;
	private	DoubleWritable	 YEAR = null	;
	private	DoubleWritable	 MONTH = null	;
	private	DoubleWritable	 DAY = null	;
	private	DoubleWritable	 TIME = null	;
	private	Text	 STATE_PROVINCE = null	;
	private	DoubleWritable	 PRIMARY_CHECKLIST_FLAG = null	;
	private	DoubleWritable	 ELEV_GT = null	;
	private	DoubleWritable	 ELEV_NED = null	;
	private	DoubleWritable	 HOUSING_DENSITY = null	;
	private	DoubleWritable	 HOUSING_PERCENT_VACANT = null	;
	private	DoubleWritable	 DIST_FROM_FLOWING_FRESH = null	;
	private	DoubleWritable	 DIST_IN_FLOWING_FRESH = null	;
	private	DoubleWritable	 DIST_FROM_STANDING_FRESH = null	;
	private	DoubleWritable	 DIST_IN_STANDING_FRESH = null	;
	private	DoubleWritable	 DIST_FROM_WET_VEG_FRESH = null	;
	private	DoubleWritable	 DIST_IN_WET_VEG_FRESH = null	;
	private	DoubleWritable	 DIST_FROM_FLOWING_BRACKISH = null	;
	private	DoubleWritable	 DIST_IN_FLOWING_BRACKISH = null	;
	private	DoubleWritable	 DIST_FROM_STANDING_BRACKISH = null	;
	private	DoubleWritable	 DIST_IN_STANDING_BRACKISH = null	;
	private	DoubleWritable	 DIST_FROM_WET_VEG_BRACKISH = null	;
	private	DoubleWritable	 DIST_IN_WET_VEG_BRACKISH = null	;
	private	BooleanWritable	 ELEV_GT_B = new BooleanWritable(true);
	private	BooleanWritable	 ELEV_NED_B = new BooleanWritable(true);
	private	BooleanWritable	 HOUSING_DENSITY_B = new BooleanWritable(true)	;
	private	BooleanWritable	 HOUSING_PERCENT_VACANT_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_FROM_FLOWING_FRESH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_IN_FLOWING_FRESH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_FROM_STANDING_FRESH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_IN_STANDING_FRESH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_FROM_WET_VEG_FRESH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_IN_WET_VEG_FRESH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_FROM_FLOWING_BRACKISH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_IN_FLOWING_BRACKISH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_FROM_STANDING_BRACKISH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_IN_STANDING_BRACKISH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_FROM_WET_VEG_BRACKISH_B = new BooleanWritable(true)	;
	private	BooleanWritable	 DIST_IN_WET_VEG_BRACKISH_B = new BooleanWritable(true)	;
	private	Text	 SampleID = null	;
	
	// init
	public SightingDetails(String[] row) {		
		
		Index	 = new IntWritable(Integer.parseInt(row[0]));	
		
		// converting required things
		if((row[1].equals("true"))||(row[1].equals("1")))
			Red_Bird= new BooleanWritable(true);	
		else
			Red_Bird= new BooleanWritable(false);
																					
		LATITUDE		 = new DoubleWritable(Double.parseDouble(row[2]));	
		LONGITUDE		 = new DoubleWritable(Double.parseDouble(row[3]));			
		YEAR		 = new DoubleWritable(Double.parseDouble(row[4]));			
		MONTH		 = new DoubleWritable(Double.parseDouble(row[5]));		
		DAY		 = new DoubleWritable(Double.parseDouble(row[6]));			
		TIME		 = new DoubleWritable(Double.parseDouble(row[7]));			
		STATE_PROVINCE		 = new Text(row[8]);		
		PRIMARY_CHECKLIST_FLAG		 = new DoubleWritable(Double.parseDouble(row[9]));		
		if(!row[10].contains("?")){
			ELEV_GT		 = new DoubleWritable(Double.parseDouble(row[10]));
		}
		else
			ELEV_GT_B = new BooleanWritable(false);
	
		if(!row[11].contains("?")){
			ELEV_NED		 = new DoubleWritable(Double.parseDouble(row[11]));	
		}
		else
			ELEV_NED_B = new BooleanWritable(false);	
		
		if(!row[12].contains("?")){
			HOUSING_DENSITY		 = new DoubleWritable(Double.parseDouble(row[12]));	
		}
		else
			HOUSING_DENSITY_B = new BooleanWritable(false);	
		
		if(!row[13].contains("?")){
			HOUSING_PERCENT_VACANT		 = new DoubleWritable(Double.parseDouble(row[13]));	
		}
		else
			HOUSING_PERCENT_VACANT_B = new BooleanWritable(false);

		if(!row[14].contains("?"))
			DIST_FROM_FLOWING_FRESH		 = new DoubleWritable(Double.parseDouble(row[14]));
		else
			DIST_FROM_FLOWING_FRESH_B = new BooleanWritable(false);

		if(!row[15].contains("?"))
			DIST_IN_FLOWING_FRESH		 = new DoubleWritable(Double.parseDouble(row[15]));	
		else
			DIST_IN_FLOWING_FRESH_B = new BooleanWritable(false);

		if(!row[16].contains("?"))
			DIST_FROM_STANDING_FRESH		 = new DoubleWritable(Double.parseDouble(row[16]));	
		else
			DIST_FROM_STANDING_FRESH_B = new BooleanWritable(false);

		if(!row[17].contains("?"))
			DIST_IN_STANDING_FRESH		 = new DoubleWritable(Double.parseDouble(row[17]));	
		else
			DIST_IN_STANDING_FRESH_B = new BooleanWritable(false);

		if(!row[18].contains("?"))
			DIST_FROM_WET_VEG_FRESH = new DoubleWritable(Double.parseDouble(row[18]));
		else
			DIST_FROM_WET_VEG_FRESH_B = new BooleanWritable(false);

		if(!row[19].contains("?"))
			DIST_IN_WET_VEG_FRESH = new DoubleWritable(Double.parseDouble(row[19]));
		else
			DIST_IN_WET_VEG_FRESH_B = new BooleanWritable(false);

		if(!row[20].contains("?"))
			DIST_FROM_FLOWING_BRACKISH = new DoubleWritable(Double.parseDouble(row[20]));
		else
			DIST_FROM_FLOWING_BRACKISH_B = new BooleanWritable(false);

		if(!row[21].contains("?"))
			DIST_IN_FLOWING_BRACKISH = new DoubleWritable(Double.parseDouble(row[21]));
		else
			DIST_IN_FLOWING_BRACKISH_B = new BooleanWritable(false);

		if(!row[22].contains("?"))
			DIST_FROM_STANDING_BRACKISH = new DoubleWritable(Double.parseDouble(row[22]));
		else
			DIST_FROM_STANDING_BRACKISH_B = new BooleanWritable(false);

		if(!row[23].contains("?"))
			DIST_IN_STANDING_BRACKISH = new DoubleWritable(Double.parseDouble(row[23]));
		else
			DIST_IN_STANDING_BRACKISH_B = new BooleanWritable(false);

		if(!row[24].contains("?"))
			DIST_FROM_WET_VEG_BRACKISH = new DoubleWritable(Double.parseDouble(row[24]));
		else
			DIST_FROM_WET_VEG_BRACKISH_B = new BooleanWritable(false);

		if(!row[25].contains("?"))
			DIST_IN_WET_VEG_BRACKISH = new DoubleWritable(Double.parseDouble(row[25]));
		else
			DIST_IN_WET_VEG_BRACKISH_B = new BooleanWritable(false);

		SampleID = new Text(row[26]);
	}
		
	
	public SightingDetails(IntWritable index, BooleanWritable Red_bird, DoubleWritable lATITUDE,
		DoubleWritable lONGITUDE, DoubleWritable yEAR, DoubleWritable mONTH, DoubleWritable dAY, DoubleWritable tIME,
		Text sTATE_PROVINCE, DoubleWritable pRIMARY_CHECKLIST_FLAG, DoubleWritable eLEV_GT, DoubleWritable eLEV_NED,
		DoubleWritable hOUSING_DENSITY, DoubleWritable hOUSING_PERCENT_VACANT, DoubleWritable dIST_FROM_FLOWING_FRESH,
		DoubleWritable dIST_IN_FLOWING_FRESH, DoubleWritable dIST_FROM_STANDING_FRESH,
		DoubleWritable dIST_IN_STANDING_FRESH, DoubleWritable dIST_FROM_WET_VEG_FRESH,
		DoubleWritable dIST_IN_WET_VEG_FRESH, DoubleWritable dIST_FROM_FLOWING_BRACKISH,
		DoubleWritable dIST_IN_FLOWING_BRACKISH, DoubleWritable dIST_FROM_STANDING_BRACKISH,
		DoubleWritable dIST_IN_STANDING_BRACKISH, DoubleWritable dIST_FROM_WET_VEG_BRACKISH,
		DoubleWritable dIST_IN_WET_VEG_BRACKISH) {
	
		Index = index;
		Red_Bird = Red_bird;
		LATITUDE = lATITUDE;
		LONGITUDE = lONGITUDE;
		YEAR = yEAR;
		MONTH = mONTH;
		DAY = dAY;
		TIME = tIME;
		STATE_PROVINCE = sTATE_PROVINCE;
		PRIMARY_CHECKLIST_FLAG = pRIMARY_CHECKLIST_FLAG;
		ELEV_GT = eLEV_GT;
		ELEV_NED = eLEV_NED;
		HOUSING_DENSITY = hOUSING_DENSITY;
		HOUSING_PERCENT_VACANT = hOUSING_PERCENT_VACANT;
		DIST_FROM_FLOWING_FRESH = dIST_FROM_FLOWING_FRESH;
		DIST_IN_FLOWING_FRESH = dIST_IN_FLOWING_FRESH;
		DIST_FROM_STANDING_FRESH = dIST_FROM_STANDING_FRESH;
		DIST_IN_STANDING_FRESH = dIST_IN_STANDING_FRESH;
		DIST_FROM_WET_VEG_FRESH = dIST_FROM_WET_VEG_FRESH;
		DIST_IN_WET_VEG_FRESH = dIST_IN_WET_VEG_FRESH;
		DIST_FROM_FLOWING_BRACKISH = dIST_FROM_FLOWING_BRACKISH;
		DIST_IN_FLOWING_BRACKISH = dIST_IN_FLOWING_BRACKISH;
		DIST_FROM_STANDING_BRACKISH = dIST_FROM_STANDING_BRACKISH;
		DIST_IN_STANDING_BRACKISH = dIST_IN_STANDING_BRACKISH;
		DIST_FROM_WET_VEG_BRACKISH = dIST_FROM_WET_VEG_BRACKISH;
		DIST_IN_WET_VEG_BRACKISH = dIST_IN_WET_VEG_BRACKISH;
	}
	// constructor
	public SightingDetails(){
		Index = null;
		Red_Bird = null;
		SampleID = null;
		LATITUDE = null;
		LONGITUDE = null;
		YEAR = null;
		MONTH = null;
		DAY = null;
		TIME = null;
		STATE_PROVINCE = null;
		PRIMARY_CHECKLIST_FLAG = null;
		ELEV_GT = null;
		ELEV_GT_B = new BooleanWritable(true);
		ELEV_NED = null;
		ELEV_NED_B = new BooleanWritable(true);
		HOUSING_DENSITY = null;
		HOUSING_DENSITY_B = new BooleanWritable(true);
		HOUSING_PERCENT_VACANT = null;
		HOUSING_PERCENT_VACANT_B = new BooleanWritable(true);
		DIST_FROM_FLOWING_FRESH = null;
		DIST_FROM_FLOWING_FRESH_B = new BooleanWritable(true);
		DIST_IN_FLOWING_FRESH = null;
		DIST_IN_FLOWING_FRESH_B = new BooleanWritable(true);
		DIST_FROM_STANDING_FRESH = null;
		DIST_FROM_STANDING_FRESH_B = new BooleanWritable(true);
		DIST_IN_STANDING_FRESH = null;
		DIST_IN_STANDING_FRESH_B = new BooleanWritable(true);
		DIST_FROM_WET_VEG_FRESH = null;
		DIST_FROM_WET_VEG_FRESH_B = new BooleanWritable(true);
		DIST_IN_WET_VEG_FRESH = null;
		DIST_IN_WET_VEG_FRESH_B = new BooleanWritable(true);
		DIST_FROM_FLOWING_BRACKISH = null;
		DIST_FROM_FLOWING_BRACKISH_B = new BooleanWritable(true);
		DIST_IN_FLOWING_BRACKISH = null;
		DIST_IN_FLOWING_BRACKISH_B = new BooleanWritable(true);
		DIST_FROM_STANDING_BRACKISH = null;
		DIST_FROM_STANDING_BRACKISH_B = new BooleanWritable(true);
		DIST_IN_STANDING_BRACKISH = null;
		DIST_IN_STANDING_BRACKISH_B = new BooleanWritable(true);
		DIST_FROM_WET_VEG_BRACKISH = null;
		DIST_FROM_WET_VEG_BRACKISH_B = new BooleanWritable(true);
		DIST_IN_WET_VEG_BRACKISH = null;
		DIST_IN_WET_VEG_BRACKISH_B = new BooleanWritable(true);
	}


	@Override
	public void readFields(DataInput dataInput) throws IOException {
		Index = new IntWritable();
		Index.readFields(dataInput);
		Red_Bird = new BooleanWritable();
		Red_Bird.readFields(dataInput);
		LATITUDE = new DoubleWritable();
		LATITUDE.readFields(dataInput);
		LONGITUDE = new DoubleWritable();
		LONGITUDE.readFields(dataInput);
		YEAR = new DoubleWritable();
		YEAR.readFields(dataInput);
		MONTH = new DoubleWritable();
		MONTH.readFields(dataInput);
		DAY = new DoubleWritable();
		DAY.readFields(dataInput);
		TIME = new DoubleWritable();
		TIME.readFields(dataInput);
		STATE_PROVINCE = new Text();
		STATE_PROVINCE.readFields(dataInput);
		PRIMARY_CHECKLIST_FLAG = new DoubleWritable();
		PRIMARY_CHECKLIST_FLAG.readFields(dataInput);
		ELEV_GT_B = new BooleanWritable();
		ELEV_GT_B.readFields(dataInput);
		ELEV_NED_B = new BooleanWritable();
		ELEV_NED_B.readFields(dataInput);
		HOUSING_DENSITY_B = new BooleanWritable();
		HOUSING_DENSITY_B.readFields(dataInput);
		HOUSING_PERCENT_VACANT_B = new BooleanWritable();
		HOUSING_PERCENT_VACANT_B.readFields(dataInput);
		DIST_FROM_FLOWING_FRESH_B = new BooleanWritable();
		DIST_FROM_FLOWING_FRESH_B.readFields(dataInput);
		DIST_IN_FLOWING_FRESH_B = new BooleanWritable();
		DIST_IN_FLOWING_FRESH_B.readFields(dataInput);
		DIST_FROM_STANDING_FRESH_B = new BooleanWritable();
		DIST_FROM_STANDING_FRESH_B.readFields(dataInput);
		DIST_IN_STANDING_FRESH_B = new BooleanWritable();
		DIST_IN_STANDING_FRESH_B.readFields(dataInput);
		DIST_FROM_WET_VEG_FRESH_B = new BooleanWritable();
		DIST_FROM_WET_VEG_FRESH_B.readFields(dataInput);
		DIST_IN_WET_VEG_FRESH_B = new BooleanWritable();
		DIST_IN_WET_VEG_FRESH_B.readFields(dataInput);
		DIST_FROM_FLOWING_BRACKISH_B = new BooleanWritable();
		DIST_FROM_FLOWING_BRACKISH_B.readFields(dataInput);
		DIST_IN_FLOWING_BRACKISH_B = new BooleanWritable();
		DIST_IN_FLOWING_BRACKISH_B.readFields(dataInput);
		DIST_FROM_STANDING_BRACKISH_B = new BooleanWritable();
		DIST_FROM_STANDING_BRACKISH_B.readFields(dataInput);
		DIST_IN_STANDING_BRACKISH_B = new BooleanWritable();
		DIST_IN_STANDING_BRACKISH_B.readFields(dataInput);
		DIST_FROM_WET_VEG_BRACKISH_B = new BooleanWritable();
		DIST_FROM_WET_VEG_BRACKISH_B.readFields(dataInput);
		DIST_IN_WET_VEG_BRACKISH_B = new BooleanWritable();
		DIST_IN_WET_VEG_BRACKISH_B.readFields(dataInput);
		if(ELEV_GT_B.get()){
			ELEV_GT = new DoubleWritable();
		 	ELEV_GT.readFields(dataInput);
		}
		if(ELEV_NED_B.get()){
			ELEV_NED = new DoubleWritable();
		 	ELEV_NED.readFields(dataInput);
		}
		if(HOUSING_DENSITY_B.get()){
			HOUSING_DENSITY = new DoubleWritable();
		 	HOUSING_DENSITY.readFields(dataInput);
		}
		if(HOUSING_PERCENT_VACANT_B.get()){
			HOUSING_PERCENT_VACANT = new DoubleWritable();
		 	HOUSING_PERCENT_VACANT.readFields(dataInput);
		}
	 	if(DIST_FROM_FLOWING_FRESH_B.get()){
	 		DIST_FROM_FLOWING_FRESH = new DoubleWritable();
			DIST_FROM_FLOWING_FRESH.readFields(dataInput);
	 	}
	 	if(DIST_IN_FLOWING_FRESH_B.get()){
	 		DIST_IN_FLOWING_FRESH = new DoubleWritable();
		 	DIST_IN_FLOWING_FRESH.readFields(dataInput);
	 	}
	 	if(DIST_FROM_STANDING_FRESH_B.get()){
	 		DIST_FROM_STANDING_FRESH = new DoubleWritable();
		 	DIST_FROM_STANDING_FRESH.readFields(dataInput);
	 	}
	 	if(DIST_IN_STANDING_FRESH_B.get()){
	 		DIST_IN_STANDING_FRESH = new DoubleWritable();
		 	DIST_IN_STANDING_FRESH.readFields(dataInput);
	 	}
	 	if(DIST_FROM_WET_VEG_FRESH_B.get()){
	 		DIST_FROM_WET_VEG_FRESH = new DoubleWritable();
		 	DIST_FROM_WET_VEG_FRESH.readFields(dataInput);
	 	}
	 	if(DIST_IN_WET_VEG_FRESH_B.get()){
	 		DIST_IN_WET_VEG_FRESH = new DoubleWritable();
		 	DIST_IN_WET_VEG_FRESH.readFields(dataInput);
	 	}
	 	if(DIST_FROM_FLOWING_BRACKISH_B.get()){
	 		DIST_FROM_FLOWING_BRACKISH = new DoubleWritable();
		 	DIST_FROM_FLOWING_BRACKISH.readFields(dataInput);
	 	}
	 	if(DIST_IN_FLOWING_BRACKISH_B.get()){
	 		DIST_IN_FLOWING_BRACKISH = new DoubleWritable();
		 	DIST_IN_FLOWING_BRACKISH.readFields(dataInput);
	 	}
	 	if(DIST_FROM_STANDING_BRACKISH_B.get()){
	 		DIST_FROM_STANDING_BRACKISH = new DoubleWritable();
		 	DIST_FROM_STANDING_BRACKISH.readFields(dataInput);
	 	}
	 	if(DIST_IN_STANDING_BRACKISH_B.get()){
	 		DIST_IN_STANDING_BRACKISH = new DoubleWritable();
		 	DIST_IN_STANDING_BRACKISH.readFields(dataInput);
	 	}
	 	if(DIST_FROM_WET_VEG_BRACKISH_B.get()){
	 		DIST_FROM_WET_VEG_BRACKISH = new DoubleWritable();
		 	DIST_FROM_WET_VEG_BRACKISH.readFields(dataInput);
	 	}
	 	if(DIST_IN_WET_VEG_BRACKISH_B.get()){
	 		DIST_IN_WET_VEG_BRACKISH = new DoubleWritable();
		 	DIST_IN_WET_VEG_BRACKISH.readFields(dataInput);
	 	}
	 	SampleID = new Text();
		SampleID.readFields(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		Index.write(dataOutput);
		// System.out.println("Index : " + Index.get());
		Red_Bird.write(dataOutput);
		LATITUDE.write(dataOutput);
		LONGITUDE.write(dataOutput);
		YEAR.write(dataOutput);
		MONTH.write(dataOutput);
		DAY.write(dataOutput);
		TIME.write(dataOutput);
		STATE_PROVINCE.write(dataOutput);
		PRIMARY_CHECKLIST_FLAG.write(dataOutput);
		ELEV_GT_B.write(dataOutput);
		ELEV_NED_B.write(dataOutput);
		HOUSING_DENSITY_B.write(dataOutput);
		HOUSING_PERCENT_VACANT_B.write(dataOutput);
		DIST_FROM_FLOWING_FRESH_B.write(dataOutput);
		DIST_IN_FLOWING_FRESH_B.write(dataOutput);
		DIST_FROM_STANDING_FRESH_B.write(dataOutput);
		DIST_IN_STANDING_FRESH_B.write(dataOutput);
		DIST_FROM_WET_VEG_FRESH_B.write(dataOutput);
		DIST_IN_WET_VEG_FRESH_B.write(dataOutput);
		DIST_FROM_FLOWING_BRACKISH_B.write(dataOutput);
		DIST_IN_FLOWING_BRACKISH_B.write(dataOutput);
		DIST_FROM_STANDING_BRACKISH_B.write(dataOutput);
		DIST_IN_STANDING_BRACKISH_B.write(dataOutput);
		DIST_FROM_WET_VEG_BRACKISH_B.write(dataOutput);
		DIST_IN_WET_VEG_BRACKISH_B.write(dataOutput);
		if(ELEV_GT_B.get())
		 	ELEV_GT.write(dataOutput);
		if(ELEV_NED_B.get())
		 	ELEV_NED.write(dataOutput);
		if(HOUSING_DENSITY_B.get())
		 	HOUSING_DENSITY.write(dataOutput);
		if(HOUSING_PERCENT_VACANT_B.get())
		 	HOUSING_PERCENT_VACANT.write(dataOutput);
	 	if(DIST_FROM_FLOWING_FRESH_B.get())
			DIST_FROM_FLOWING_FRESH.write(dataOutput);
	 	if(DIST_IN_FLOWING_FRESH_B.get())
		 	DIST_IN_FLOWING_FRESH.write(dataOutput);
	 	if(DIST_FROM_STANDING_FRESH_B.get())
		 	DIST_FROM_STANDING_FRESH.write(dataOutput);
	 	if(DIST_IN_STANDING_FRESH_B.get())
		 	DIST_IN_STANDING_FRESH.write(dataOutput);
	 	if(DIST_FROM_WET_VEG_FRESH_B.get())
		 	DIST_FROM_WET_VEG_FRESH.write(dataOutput);
	 	if(DIST_IN_WET_VEG_FRESH_B.get())
		 	DIST_IN_WET_VEG_FRESH.write(dataOutput);
	 	if(DIST_FROM_FLOWING_BRACKISH_B.get())
		 	DIST_FROM_FLOWING_BRACKISH.write(dataOutput);
	 	if(DIST_IN_FLOWING_BRACKISH_B.get())
		 	DIST_IN_FLOWING_BRACKISH.write(dataOutput);
	 	if(DIST_FROM_STANDING_BRACKISH_B.get())
		 	DIST_FROM_STANDING_BRACKISH.write(dataOutput);
	 	if(DIST_IN_STANDING_BRACKISH_B.get())
		 	DIST_IN_STANDING_BRACKISH.write(dataOutput);
	 	if(DIST_FROM_WET_VEG_BRACKISH_B.get())
		 	DIST_FROM_WET_VEG_BRACKISH.write(dataOutput);
	 	if(DIST_IN_WET_VEG_BRACKISH_B.get())
		 	DIST_IN_WET_VEG_BRACKISH.write(dataOutput);	
		SampleID.write(dataOutput);

	}

	// getter setter
	public IntWritable getIndex() {
		return Index;
	}
	public void setIndex(IntWritable index) {
		Index = index;
	}
	public BooleanWritable getRed_Bird() {
		return Red_Bird;
	}
	public void setRed_Bird(BooleanWritable Red_bird) {
		Red_Bird = Red_bird;
	}
	public DoubleWritable getLATITUDE() {
		return LATITUDE;
	}
	public void setLATITUDE(DoubleWritable lATITUDE) {
		LATITUDE = lATITUDE;
	}
	public DoubleWritable getLONGITUDE() {
		return LONGITUDE;
	}
	public void setLONGITUDE(DoubleWritable lONGITUDE) {
		LONGITUDE = lONGITUDE;
	}
	public DoubleWritable getYEAR() {
		return YEAR;
	}
	public void setYEAR(DoubleWritable yEAR) {
		YEAR = yEAR;
	}
	public DoubleWritable getMONTH() {
		return MONTH;
	}
	public void setMONTH(DoubleWritable mONTH) {
		MONTH = mONTH;
	}
	public DoubleWritable getDAY() {
		return DAY;
	}
	public void setDAY(DoubleWritable dAY) {
		DAY = dAY;
	}
	public DoubleWritable getTIME() {
		return TIME;
	}
	public void setTIME(DoubleWritable tIME) {
		TIME = tIME;
	}
	public Text getSTATE_PROVINCE() {
		return STATE_PROVINCE;
	}
	public Text getSampleId() {
		return SampleID;
	}
	public void setSTATE_PROVINCE(Text sTATE_PROVINCE) {
		STATE_PROVINCE = sTATE_PROVINCE;
	}
	public DoubleWritable getPRIMARY_CHECKLIST_FLAG() {
		return PRIMARY_CHECKLIST_FLAG;
	}
	public void setPRIMARY_CHECKLIST_FLAG(DoubleWritable pRIMARY_CHECKLIST_FLAG) {
		PRIMARY_CHECKLIST_FLAG = pRIMARY_CHECKLIST_FLAG;
	}
	public DoubleWritable getELEV_GT() {
		return ELEV_GT;
	}
	public void setELEV_GT(DoubleWritable eLEV_GT) {
		ELEV_GT = eLEV_GT;
	}
	public DoubleWritable getELEV_NED() {
		return ELEV_NED;
	}
	public void setELEV_NED(DoubleWritable eLEV_NED) {
		ELEV_NED = eLEV_NED;
	}
	public DoubleWritable getHOUSING_DENSITY() {
		return HOUSING_DENSITY;
	}
	public void setHOUSING_DENSITY(DoubleWritable hOUSING_DENSITY) {
		HOUSING_DENSITY = hOUSING_DENSITY;
	}
	public DoubleWritable getHOUSING_PERCENT_VACANT() {
		return HOUSING_PERCENT_VACANT;
	}
	public void setHOUSING_PERCENT_VACANT(DoubleWritable hOUSING_PERCENT_VACANT) {
		HOUSING_PERCENT_VACANT = hOUSING_PERCENT_VACANT;
	}
	public DoubleWritable getDIST_FROM_FLOWING_FRESH() {
		return DIST_FROM_FLOWING_FRESH;
	}
	public void setDIST_FROM_FLOWING_FRESH(DoubleWritable dIST_FROM_FLOWING_FRESH) {
		DIST_FROM_FLOWING_FRESH = dIST_FROM_FLOWING_FRESH;
	}
	public DoubleWritable getDIST_IN_FLOWING_FRESH() {
		return DIST_IN_FLOWING_FRESH;
	}
	public void setDIST_IN_FLOWING_FRESH(DoubleWritable dIST_IN_FLOWING_FRESH) {
		DIST_IN_FLOWING_FRESH = dIST_IN_FLOWING_FRESH;
	}
	public DoubleWritable getDIST_FROM_STANDING_FRESH() {
		return DIST_FROM_STANDING_FRESH;
	}
	public void setDIST_FROM_STANDING_FRESH(DoubleWritable dIST_FROM_STANDING_FRESH) {
		DIST_FROM_STANDING_FRESH = dIST_FROM_STANDING_FRESH;
	}
	public DoubleWritable getDIST_IN_STANDING_FRESH() {
		return DIST_IN_STANDING_FRESH;
	}
	public void setDIST_IN_STANDING_FRESH(DoubleWritable dIST_IN_STANDING_FRESH) {
		DIST_IN_STANDING_FRESH = dIST_IN_STANDING_FRESH;
	}
	public DoubleWritable getDIST_FROM_WET_VEG_FRESH() {
		return DIST_FROM_WET_VEG_FRESH;
	}
	public void setDIST_FROM_WET_VEG_FRESH(DoubleWritable dIST_FROM_WET_VEG_FRESH) {
		DIST_FROM_WET_VEG_FRESH = dIST_FROM_WET_VEG_FRESH;
	}
	public DoubleWritable getDIST_IN_WET_VEG_FRESH() {
		return DIST_IN_WET_VEG_FRESH;
	}
	public void setDIST_IN_WET_VEG_FRESH(DoubleWritable dIST_IN_WET_VEG_FRESH) {
		DIST_IN_WET_VEG_FRESH = dIST_IN_WET_VEG_FRESH;
	}
	public DoubleWritable getDIST_FROM_FLOWING_BRACKISH() {
		return DIST_FROM_FLOWING_BRACKISH;
	}
	public void setDIST_FROM_FLOWING_BRACKISH(DoubleWritable dIST_FROM_FLOWING_BRACKISH) {
		DIST_FROM_FLOWING_BRACKISH = dIST_FROM_FLOWING_BRACKISH;
	}
	public DoubleWritable getDIST_IN_FLOWING_BRACKISH() {
		return DIST_IN_FLOWING_BRACKISH;
	}
	public void setDIST_IN_FLOWING_BRACKISH(DoubleWritable dIST_IN_FLOWING_BRACKISH) {
		DIST_IN_FLOWING_BRACKISH = dIST_IN_FLOWING_BRACKISH;
	}
	public DoubleWritable getDIST_FROM_STANDING_BRACKISH() {
		return DIST_FROM_STANDING_BRACKISH;
	}
	public void setDIST_FROM_STANDING_BRACKISH(DoubleWritable dIST_FROM_STANDING_BRACKISH) {
		DIST_FROM_STANDING_BRACKISH = dIST_FROM_STANDING_BRACKISH;
	}
	public DoubleWritable getDIST_IN_STANDING_BRACKISH() {
		return DIST_IN_STANDING_BRACKISH;
	}
	public void setDIST_IN_STANDING_BRACKISH(DoubleWritable dIST_IN_STANDING_BRACKISH) {
		DIST_IN_STANDING_BRACKISH = dIST_IN_STANDING_BRACKISH;
	}
	public DoubleWritable getDIST_FROM_WET_VEG_BRACKISH() {
		return DIST_FROM_WET_VEG_BRACKISH;
	}
	public void setDIST_FROM_WET_VEG_BRACKISH(DoubleWritable dIST_FROM_WET_VEG_BRACKISH) {
		DIST_FROM_WET_VEG_BRACKISH = dIST_FROM_WET_VEG_BRACKISH;
	}
	public DoubleWritable getDIST_IN_WET_VEG_BRACKISH() {
		return DIST_IN_WET_VEG_BRACKISH;
	}
	public void setDIST_IN_WET_VEG_BRACKISH(DoubleWritable dIST_IN_WET_VEG_BRACKISH) {
		DIST_IN_WET_VEG_BRACKISH = dIST_IN_WET_VEG_BRACKISH;
	}

	public BooleanWritable getELEV_GT_B() {
		return ELEV_GT_B;
	}
	
	public BooleanWritable getELEV_NED_B() {
		return ELEV_NED_B;
	}
	
	public BooleanWritable getHOUSING_DENSITY_B() {
		return HOUSING_DENSITY_B;
	}
	
	public BooleanWritable getHOUSING_PERCENT_VACANT_B() {
		return HOUSING_PERCENT_VACANT_B;
	}
	
	public BooleanWritable getDIST_FROM_FLOWING_FRESH_B() {
		return DIST_FROM_FLOWING_FRESH_B;
	}
	
	public BooleanWritable getDIST_IN_FLOWING_FRESH_B() {
		return DIST_IN_FLOWING_FRESH_B;
	}
	
	public BooleanWritable getDIST_FROM_STANDING_FRESH_B() {
		return DIST_FROM_STANDING_FRESH_B;
	}
	
	public BooleanWritable getDIST_IN_STANDING_FRESH_B() {
		return DIST_IN_STANDING_FRESH_B;
	}
	
	public BooleanWritable getDIST_FROM_WET_VEG_FRESH_B() {
		return DIST_FROM_WET_VEG_FRESH_B;
	}
	
	public BooleanWritable getDIST_IN_WET_VEG_FRESH_B() {
		return DIST_IN_WET_VEG_FRESH_B;
	}
	
	public BooleanWritable getDIST_FROM_FLOWING_BRACKISH_B() {
		return DIST_FROM_FLOWING_BRACKISH_B;
	}
	
	public BooleanWritable getDIST_IN_FLOWING_BRACKISH_B() {
		return DIST_IN_FLOWING_BRACKISH_B;
	}
	
	public BooleanWritable getDIST_FROM_STANDING_BRACKISH_B() {
		return DIST_FROM_STANDING_BRACKISH_B;
	}
	
	public BooleanWritable getDIST_IN_STANDING_BRACKISH_B() {
		return DIST_IN_STANDING_BRACKISH_B;
	}
	
	public BooleanWritable getDIST_FROM_WET_VEG_BRACKISH_B() {
		return DIST_FROM_WET_VEG_BRACKISH_B;
	}
	
	public BooleanWritable getDIST_IN_WET_VEG_BRACKISH_B() {
		return DIST_IN_WET_VEG_BRACKISH_B;
	}
}





