package org.masterbigdata.mapreduce;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 * @author Antonio J. Nebro
 * This class implements the classic Hadoop word count program
 */
public class AddNumbers {

  public static class Map 
  extends MapReduceBase 
  implements Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    
    public void map(LongWritable key, 
        Text value, 
        OutputCollector<Text, IntWritable> output, 
        Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      int suma = 0;
      while (tokenizer.hasMoreTokens()) {
        suma += Integer.parseInt(tokenizer.nextToken());
      }
      IntWritable resp = new IntWritable();
      resp.set(suma);
      output.collect(word, resp);
    }
  }

  public static class Reduce 
  extends MapReduceBase 
  implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, 
        Iterator<IntWritable> values, 
        OutputCollector<Text, IntWritable> output, 
        Reporter reporter) 
            throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(new Text("Total Sum"), new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(AddNumbers.class);
    conf.setJobName("wordcount");

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
