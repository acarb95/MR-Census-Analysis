
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Q8MapperPart2 extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	NullWritable val = NullWritable.get();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\t");
		
		String state = split[0];
		double elderly = Double.parseDouble(split[1]);
		
		context.write(new Text(state + ":" + elderly), val);
	}
}
