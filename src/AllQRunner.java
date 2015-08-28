import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Objects.ValueWritable;

public class AllQRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "HW3Runner");
		job.setJarByClass(AllQRunner.class);
		job.setCombinerClass(AllQCombiner.class);
		job.setMapperClass(AllQMapper.class);
		job.setReducerClass(AllQReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ValueWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		/*
		 * NOTE: The part 2 for Q8 could be merged with the original map reduce program by 
		 * forcing sorting on ALL keys. There would need to be extra code in the comparator
		 * to return 0 if it's not from Q8 and return the double comparison if it is. The grouper
		 * would need to group all the keys by question number and state. Part 2 of Q7 could be merged as well, 
		 * but would require more work. The grouper would then need to group all keys by question number
		 * and state except for Q7, it would need to group all of Q7 together. The values for Q7 would
		 * be changed from rooms to rooms and state. The reducer would then have to average all the rooms per state,
		 * then add them to an array to calculate the 95th percentile. This seems like a lot more work to force
		 * it into one program than to have an elegant and easy to understand second job to calculate it.
		 * 
		 * I kept them as separate jobs because more information is better than less. A user might
		 * think they only want the state with the highest percentage of elderly and the 95th percentile
		 * for average rooms, but what if after seeing the statistic and believing it is inaccurate, they
		 * want to see the average number of rooms per state and the percent of elderly per state? Now they
		 * have that intermediate output to verify the result with. 
		 * 
		 * Originally I was going to use two java methods to calculate the final result for Q7 and Q8 because there are
		 * only 50 lines in the file, therefore it didn't seem necessary to use map reduce for it. But the constraints
		 * of this assignment require that it is all implemented in map reduce. I have kept my helper methods for reference.
		 */
		Configuration confQ7p2 = new Configuration();
		Job jobQ7p2 = Job.getInstance(confQ7p2, "Q7 Part2");
		jobQ7p2.setJarByClass(AllQRunner.class);
		jobQ7p2.setMapperClass(Q7MapperPart2.class);
		jobQ7p2.setReducerClass(Q7ReducerPart2.class);
		jobQ7p2.setMapOutputKeyClass(IntWritable.class);
		jobQ7p2.setMapOutputValueClass(DoubleWritable.class);
		jobQ7p2.setOutputKeyClass(Text.class);
		jobQ7p2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(jobQ7p2, new Path(args[1] + "/Q7/"));
		FileOutputFormat.setOutputPath(jobQ7p2, new Path(args[1] + "/Q7Result/"));
		jobQ7p2.waitForCompletion(true);
		
		Configuration confQ8p2 = new Configuration();
		Job jobQ8p2 = Job.getInstance(confQ8p2, "Q8 Part2");
		jobQ8p2.setSortComparatorClass(CompositeKeyComparator.class);
		jobQ8p2.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
		jobQ8p2.setJarByClass(AllQRunner.class);
		jobQ8p2.setMapperClass(Q8MapperPart2.class);
		jobQ8p2.setReducerClass(Q8ReducerPart2.class);
		jobQ8p2.setMapOutputKeyClass(Text.class);
		jobQ8p2.setMapOutputValueClass(NullWritable.class);
		jobQ8p2.setOutputKeyClass(Text.class);
		jobQ8p2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(jobQ8p2, new Path(args[1] + "/Q8/"));
		FileOutputFormat.setOutputPath(jobQ8p2, new Path(args[1] + "/Q8Result/"));
		jobQ8p2.waitForCompletion(true);
		
		// Call java method to finish Q7 and Q8
		//completeQ7Part2(conf, "viewfs:///" + args[1]);
		//completeQ8Part2(conf, "viewfs:///" + args[1]);
	}
/*
	private static void completeQ7Part2(Configuration conf, String directory)
			throws IOException {
		Path path = new Path(directory + "/Q7-r-00000");
		FileSystem system = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				system.open(path)));

		ArrayList<Double> data = new ArrayList<Double>();

		String line;
		line = reader.readLine();
		while (line != null) {
			String[] split = line.split("\t");

			data.add(Double.parseDouble(split[1]));
			
			line = reader.readLine();
		}

		Collections.sort(data);

		int index = (int) Math.round((double) data.size() * 0.95);

		if (index >= data.size()) {
			index = data.size() - 1;
		}

		Path pt = new Path(directory + "/Q7Result");
		FileSystem fs = FileSystem.get(conf);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
		br.write("95th percentile of the average number of rooms per house across the US: " + data.get(index));
		br.close();
	}
	
	private static void completeQ8Part2(Configuration conf, String directory) throws IOException {
		Path path = new Path(directory + "/Q8-r-00000");
		FileSystem system = FileSystem.get(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(system.open(path)));

		SortedMap<Double, String> data = new TreeMap<Double, String>();

		String line;
		line = reader.readLine();
		while (line != null) {
			String[] split = line.split("\t");

			data.put(Double.parseDouble(split[1]), split[0].replace(":", ""));
			
			line = reader.readLine();
		}

		String state = data.get(data.lastKey());
		Path pt = new Path(directory + "/Q8Result");
		FileSystem fs = FileSystem.get(conf);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
		br.write("US state with the highest percentage of elderly : " + state + " with " + data.lastKey() + "%");
		br.close();
	}
*/
}
