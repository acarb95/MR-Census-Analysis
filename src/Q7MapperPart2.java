
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q7MapperPart2 extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
	IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		
		Double averageRooms = Double.parseDouble(values[1]);
		
		// Send 1 so all the average room counts go to the same reducer. No need to keep track of state
		context.write(one, new DoubleWritable(averageRooms));
	}
}
