package org.masterbigdata.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author Gabriel Requena. Implementation of the SpanishAirports Task.
 */
public class SpanishAirports {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String data = value.toString();

			// we separate each line
			String[] lines = data.split("/n");

			// iterate in each line
			for (String line : lines) {

				// we isolate the columns
				String[] columns = line.split(",");

				// we assure ourselves that the line has more than 8 columns,
				// and then we compare if the 9th position (corresponding to the
				// country label) is Spain ("ES")
				if (columns.length > 8 && columns[8].equals("\"ES\"")) {
					// we check that the type of airport equals to one of the four types specified and use it as the key to
					// inject in the reduce stage
					if (columns[2].equals("\"heliport\"")
							|| columns[2].equals("\"small_airport\"")
							|| columns[2].equals("\"large_airport\"")
							|| columns[2].equals("\"medium_airport\"")) {
						word.set(columns[2]);
						output.collect(word, one);
					}
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SpanishAirports.class);
		conf.setJobName("spanishairports");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}
