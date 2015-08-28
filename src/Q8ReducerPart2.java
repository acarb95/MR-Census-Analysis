
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Q8ReducerPart2 extends Reducer <Text, NullWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		String state = key.toString().split(":")[0];
		double percentage = Double.parseDouble(key.toString().split(":")[1]);
		context.write(new Text("State with the highest elderly percentage: \t" + state + " with"), new DoubleWritable(percentage));
	}
}
