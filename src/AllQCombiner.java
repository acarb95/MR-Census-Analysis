import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Objects.ValueWritable;


public class AllQCombiner extends Reducer <Text, ValueWritable, Text, ValueWritable> {

	public void reduce(Text key, Iterable<ValueWritable> values, Context context) throws IOException, InterruptedException {
		String question_num = key.toString().split(":")[1];
		
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
			context.write(key, new ValueWritable(val1, val2, val3, total));
			break;
		case "Q5":
		case "Q6":
			double[] list = new double[20];
			for (ValueWritable val : values) {
				list[(int) val.getVal1()] += val.getTotal();
			}
			
			for (int i = 0; i < list.length; i++) {
				if (list[i] > 0) {
					context.write(key, new ValueWritable(i, list[i]));
				}
			}
			break;
		case "Q7":
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
			
			for (Integer room : data.keySet()) {
				context.write(key, new ValueWritable(room, data.get(room)));
			}
			break;
		case "Q8":
			double q8val1 = 0;
			double q8total = 0;
			for (ValueWritable val : values) {
				q8val1 += val.getVal1();
				q8total += val.getTotal();
			}
			context.write(key, new ValueWritable(q8val1, q8total));
			break;
		}
	}
}
