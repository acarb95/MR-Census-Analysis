import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import Objects.ValueWritable;


public class AllQReducer extends Reducer <Text, ValueWritable, Text, Text> {
	private static String[] rangesOwn = { "Less than $15,000",
		"$15,000 - $19,999", "$20,000 - $24,999", "$25,000 - $29,999",
		"$30,000 - $34,999", "$35,000 - $39,999", "$40,000 - $44,999",
		"$45,000 - $49,999", "$50,000 - $59,999", "$60,000 - $74,999",
		"$75,000 - $99,999", "$100,000 - $124,999", "$125,000 - $149,999",
		"$150,000 - $174,999", "$175,000 - $199,999", "$200,000 - $249,999",
		"$250,000 - $299,999", "$300,000 - $399,999", "$400,000 - $499,999", 
		"More than $500,000"};
	
	private static String[] rangesRent = { "Less than $100",
		"$100 - $149", "$150 - $199", "$200 - $249",
		"$250 - $299", "$300 - $349", "$350 - $399",
		"$400 - $449", "$450 - $499", "$500 - $549",
		"$550 - $599", "$600 - $649", "$650 - $699",
		"$700 - $749", "$750 - $999", "$1000 or more", 
		"No cash rent"};
	
	private MultipleOutputs<Text, Text> multipleOutputs;
	
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<ValueWritable> values, Context context) throws IOException, InterruptedException {
		String question_num = key.toString().split(":")[1];
		String directory = question_num + "/" + question_num;
		switch(question_num) {
			case "Q1":
			case "Q2":
			case "Q3":
			case "Q4":
				double val1 = 0;
				double val2 = 0;
				double val3 = 0;
				double total = 0;
				for (ValueWritable val : values) {
					val1 += val.getVal1();
					val2 += val.getVal2();
					val3 += val.getVal3();
					total += val.getTotal();
				}
				switch(question_num) {
				case "Q1":
					double q1val1percent = val1/total;
					double q1val2percent = val2/total;
					multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Rented %:"), new Text("" + q1val1percent*100), directory);
					multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Owned %:"), new Text("" + q1val2percent*100), directory);
					break;
				case "Q4":
					double q4val1percent = val1/total;
					double q4val2percent = val2/total;
					multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Urban %:"), new Text("" + q4val1percent*100), directory);
					multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Rural %:"), new Text("" + q4val2percent*100), directory);
					break;
				case "Q3":
					double set1percent = val1/total;
					double set2percent = val2/total;
					double set3percent = val3/total;
					
					if (key.toString().contains("female")) {
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Females Under 18 %:"), new Text("" + set1percent*100), directory);
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Females 19 - 29 %:"), new Text("" + set2percent*100), directory);
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Females 30 - 39 %:"), new Text("" + set3percent*100), directory);					
					} else {
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Males Under 18 %:"), new Text("" + set1percent*100), directory);
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Males 19 - 29 %:"), new Text("" + set2percent*100), directory);
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Males 30 - 39 %:"), new Text("" + set3percent*100), directory);
					}
					break;
				case "Q2":
					double percent = val1/total;
					if (key.toString().contains("female")) {
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Females Never Married %:") , new Text("" + percent*100), directory);
					} else {
						multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Males Never Married %:") , new Text("" + percent*100), directory);
					}
					break;
				}
				break;
			case "Q5":
			case "Q6":
				ArrayList<Integer> list_of_Q5 = new ArrayList<Integer>();
				ArrayList<Integer> list_of_Q6 = new ArrayList<Integer>();
				for (ValueWritable val : values) {
					if (question_num.equalsIgnoreCase("Q5")) {
						for (int i = 0; i < val.getTotal(); i++) {
							list_of_Q5.add((int) val.getVal1());
						}
					} else {
						for (int i = 0; i < val.getTotal(); i++) {
							list_of_Q6.add((int) val.getVal1());						
						}
					}
				}
				
				Collections.sort(list_of_Q5);
				Collections.sort(list_of_Q6);
				int index;
				double length;
				
				if (question_num.equalsIgnoreCase("Q5")) {
					length = list_of_Q5.size();
				} else {
					length = list_of_Q6.size();
				}
				
				if (length % 2 == 1) {
					index = (int) Math.round((length/2.0));
				} else {
					index = (int) length/2 + 1;
				}
				
				if (question_num.equalsIgnoreCase("Q5")) {
					multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Median Owner-Occupied Value:"), new Text(rangesOwn[list_of_Q5.get(index)]), directory);
				} else if (question_num.equalsIgnoreCase("Q6")) {
					multipleOutputs.write(new Text(key.toString().substring(0, key.toString().indexOf(":")) + " Median Contract Rent:"), new Text(rangesRent[list_of_Q6.get(index)]), directory);
				}
				break;
			case "Q7":
				// Number of rooms, Number of houses
				HashMap<Integer, Integer> data = new HashMap<Integer, Integer>();
				for (ValueWritable val : values) {
					int roomcount = (int) val.getVal1();
					int numHouses = (int) val.getTotal();
					
					if (data.containsKey(roomcount)) {
						data.put(roomcount, data.get(roomcount)+numHouses);
					} else {
						data.put(roomcount, numHouses);
					}
				}
				
				double totalHouses = 0;
				double totalRooms = 0;
				
				for (Integer room : data.keySet()) {
					totalHouses += data.get(room);
					totalRooms += data.get(room)*room;
				}
				
				double average = totalRooms/totalHouses;
				
				String state = key.toString().split(":")[0];
				
				multipleOutputs.write(new Text(state), new Text(""+ average), directory);
				break;
			case "Q8":
				double q8val1 = 0;
				double q8total = 0;
				for (ValueWritable val : values) {
					q8val1 += val.getVal1();
					q8total += val.getTotal();
				}
	
				double q8percent = q8val1/q8total;
				
				String q8state = key.toString().split(":")[0];
				
				multipleOutputs.write(new Text(q8state), new Text("" + q8percent*100), directory);
				break;
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		multipleOutputs.close();
	}
}
