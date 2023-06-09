import java.io.*;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20201037 {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		private Text word1 = new Text();
		private Text word2 = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String region = itr.nextToken();
			String date = itr.nextToken();
			String vehicle = itr.nextToken();
        		String trip = itr.nextToken();
			
			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			
			itr = new StringTokenizer(date, "/");
			int month = Integer.parseInt(itr.nextToken());
			int day = Integer.parseInt(itr.nextToken());
			int year = Integer.parseInt(itr.nextToken());

			LocalDate datee = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = datee.getDayOfWeek();
			int dayOfWeekNumber = dayOfWeek.getValue();
		       String dateStr = days[dayOfWeekNumber - 1];

			word1.set(region + "," + dateStr);
			word2.set(trip + "," + vehicle);
			context.write(word1, word2);
		}
	}

	public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long sumtrip = 0;
			long sumvehicle = 0;

			for (Text val : values) {
				String v = val.toString();
				StringTokenizer itr = new StringTokenizer(v, ",");
				sumtrip += Long.parseLong(itr.nextToken());
				sumvehicle += Long.parseLong(itr.nextToken());
			}
			result.set(sumtrip + "," + sumvehicle);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20201037");
		job.setJarByClass(UBERStudent20201037.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
