import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Objects.ValueWritable;


public class AllQMapper extends Mapper<LongWritable, Text, Text, ValueWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String values = value.toString();
		
		String state = values.substring(8, 10);
		
		int summary_level = Integer.parseInt(values.substring(10, 13));
		
		int part = Integer.parseInt(values.substring(24, 28));
		
		if (summary_level == 100 && part == 2) {
			//------------------------------------ Q1 ------------------------------------
			double owned = Double.parseDouble(values.substring(1803, 1812));
			double rented = Double.parseDouble(values.substring(1812, 1821));
			// Value 1 = rented
			// Value 2 = owned
			context.write(new Text(state+":Q1"), new ValueWritable(rented, owned, owned+rented));
			
			//------------------------------------ Q4 ------------------------------------
			double urban_in = Double.parseDouble(values.substring(1857, 1866));
			double urban_out = Double.parseDouble(values.substring(1866, 1875));
			double rural = Double.parseDouble(values.substring(1875, 1884));
			double NA = Double.parseDouble(values.substring(1884, 1893));
			
			// Value 1 = urban
			// Value 2 = rural
			context.write(new Text(state+":Q4"), new ValueWritable(urban_in+urban_out, rural, urban_in+urban_out+rural+NA));
			
			//------------------------------------ Q5 ------------------------------------
			int startIndex = 2928;
			
			for (int i = 0; i < 20; i++) {
				int val = Integer.parseInt(values.substring(startIndex, startIndex+9));
				if (val != 0) {
					context.write(new Text(state + ":Q5"), new ValueWritable(i, val));
				}
				startIndex += 9;
			}
			
			//------------------------------------ Q6 ------------------------------------
			startIndex = 3450;
			
			for (int i = 0; i < 17; i++) {
				int val = Integer.parseInt(values.substring(startIndex, startIndex+9));
				if (val != 0) {
					context.write(new Text(state + ":Q6"), new ValueWritable(i, val));
				}
				startIndex += 9;
			}
			
			//------------------------------------ Q7 ------------------------------------
			int roomCount = 1;
			for (int startindex = 2388; startindex < 2469; startindex += 9) {
				int numHouses = Integer.parseInt(values.substring(startindex, startindex + 9));
				if (numHouses != 0) {
					context.write(new Text(state + ":Q7"), new ValueWritable(roomCount, numHouses));
				}
				roomCount++;
			}
		} else if (summary_level == 100 && part == 1) {
			//------------------------------------ Q2 ------------------------------------
			// Part 1 parsing here

			int male_total = Integer.parseInt(values.substring(4422, 4431)) + Integer.parseInt(values.substring(4431, 4440)) + Integer.parseInt(values.substring(4440, 4449)) + Integer.parseInt(values.substring(4449, 4458));
			int female_total = Integer.parseInt(values.substring(4467, 4476)) + Integer.parseInt(values.substring(4476, 4485)) + Integer.parseInt(values.substring(4485, 4494)) + Integer.parseInt(values.substring(4494, 4503));
			// Value 1 = Never married population
			// Total = total population
			ValueWritable val_male = new ValueWritable(Double.parseDouble(values.substring(4422, 4431)), (double) male_total);
			context.write(new Text(state + ":Q2:males"), val_male);
		
			// Value 1 = Never married population
			// Total = total population
			ValueWritable val_female = new ValueWritable(Double.parseDouble(values.substring(4467, 4476)), (double) female_total);
			context.write(new Text(state + ":Q2:females"), val_female);
			
			//------------------------------------ Q3 ------------------------------------
			
			// Males
			// Set1 = 18 and younger values
			int set1index_male = 3864;
			// Set2 = 19-29 values
			int set2index_male = 3981;
			// Set3 = 30-39 values
			int set3index_male = 4026;
			// Total = All ages
			
			// total = 3864 - 4143 males
			// total = 4143 - 4422 females
			
			boolean set2done_male = false;
			boolean set3done_male = false;
			boolean set1done_male = false;
			
			for (int i = 3864; i < 4143; i+=9) {
				double total_male = 0;
				double set1_male = 0;
				double set2_male = 0;
				double set3_male = 0;
				
				if (!set1done_male) {
					set1_male = Double.parseDouble(values.substring(set1index_male, set1index_male+9));
					set1index_male += 9;
				}
				
				if (!set2done_male) {
					set2_male = Double.parseDouble(values.substring(set2index_male, set2index_male+9));
					set2index_male += 9;
				}
				
				if (!set3done_male) {
					set3_male = Double.parseDouble(values.substring(set3index_male, set3index_male+9));
					set3index_male += 9;
				}
				
				if (set1index_male == 3981){
					set1done_male = true;
				}
				
				if (set2index_male == 4026) {
					set2done_male = true;
				}
				
				if (set3index_male == 4044) {
					set3done_male = true;
				}
				
				total_male = Double.parseDouble(values.substring(i, i+9));
				
				// Send one value from each set (0 if the set is done)
				// These will be summed in the combiner and reducer
				context.write(new Text(state + ":Q3:males"), new ValueWritable(set1_male, set2_male, set3_male, total_male));
			}
			
			// Females
			// Set1 = 18 and younger values
			int set1index_female = 4143;
			// Set2 = 19-29 values
			int set2index_female = 4260;
			// Set3 = 30-39 values
			int set3index_female = 4305;
			// Total = All ages
			
			// total = 3864 - 4143 males
			// total = 4143 - 4422 females
			
			boolean set2done_female = false;
			boolean set3done_female = false;
			boolean set1done_female = false;
			
			for (int i = 4143; i < 4422; i+=9) {
				double total_female = 0;
				double set1_female = 0;
				double set2_female = 0;
				double set3_female = 0;
				
				if (!set1done_female) {
					set1_female = Double.parseDouble(values.substring(set1index_female, set1index_female+9));
					set1index_female += 9;
				}
				
				if (!set2done_female) {
					set2_female = Double.parseDouble(values.substring(set2index_female, set2index_female+9));
					set2index_female += 9;
				}
				
				if (!set3done_female) {
					set3_female = Double.parseDouble(values.substring(set3index_female, set3index_female+9));
					set3index_female += 9;
				}
				
				if (set1index_female == 4260){
					set1done_female = true;
				}
				
				if (set2index_female == 4305) {
					set2done_female = true;
				}
				
				if (set3index_female == 4323) {
					set3done_female = true;
				}
				
				total_female = Double.parseDouble(values.substring(i, i+9));
				
				// Send one value from each set (0 if the set is done)
				// These will be summed in the combiner and reducer
				context.write(new Text(state + ":Q3:females"), new ValueWritable(set1_female, set2_female, set3_female, total_female));
			}
			
			//------------------------------------ Q8 ------------------------------------
			double totalpop = Double.parseDouble(values.substring(300, 309));
			double elderly = Double.parseDouble(values.substring(1065, 1074));
			
			context.write(new Text(state + ":Q8"), new ValueWritable(elderly, totalpop));
		}
	}
}
