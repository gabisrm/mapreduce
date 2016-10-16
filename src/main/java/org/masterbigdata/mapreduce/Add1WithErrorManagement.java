package org.masterbigdata.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author Gabriel Requena García. Implementation of Add1WithErrorManagement assignment
 */
public class Add1WithErrorManagement {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output,
				Reporter reporter) throws IOException {
			String line = value.toString();
			
			//we tokenize the data so that multiple numbers in a line separated by spaces count as individual numbers
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String numValue = tokenizer.nextToken();
				
				//we cast a try-block statement to catch NumberFormatExcepton errors when parsing the value
				try{
					IntWritable clave = new IntWritable();
					IntWritable valor = new IntWritable();
					clave.set(Integer.parseInt(numValue));
					valor.set(Integer.parseInt(numValue) + 1);
					output.collect(clave, valor);
				} catch(NumberFormatException e){
					System.out.println("Not a valid Integer");
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Add1WithErrorManagement.class);
		conf.setJobName("sumar1");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}