
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q7ReducerPart2 extends Reducer <IntWritable, DoubleWritable, Text, DoubleWritable> {

	public void reduce (IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// Add all the data points to an arraylist
		ArrayList<Double> data = new ArrayList<Double>();
		for (DoubleWritable val : values) {
			double value = val.get();
			data.add(value);
		}
		
		// Sort that array list
		Collections.sort(data);
		
		// Calculate the 95th percentile index.
		int index = (int) Math.round((double) data.size() * 0.95);
		
		// If the index is greater than the size (should only happen on SMALL datasets)
		if (index >= data.size()) {
			// Set it to the last index
			index = data.size() - 1;
		}
		
		context.write(new Text("95th percentile of the average number of rooms per house across the US: "), new DoubleWritable(data.get(index)));
	}
}
