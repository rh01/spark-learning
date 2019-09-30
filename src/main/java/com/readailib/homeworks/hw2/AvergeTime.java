package com.readailib.homeworks.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * @program: hadoop
 * @description: homework2
 * @Author: ReadAILib
 * @create: 2018-04-12 12:09
 **/
public class AvergeTime {

    // This is the Mapper class
    // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
    //
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        private Text srcDest = new Text();
        private FloatWritable time = new FloatWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            // handle wrong format line.
            if (itr.countTokens() == 3) {
                while (itr.hasMoreTokens()) {

                    String src = itr.nextToken();
                    String dest = itr.nextToken();
                    // handle three dedcimal
                    String formatTime = itr.nextToken();
                    srcDest.set(src + " " + dest);
                    time.set(Float.parseFloat(formatTime));
                    context.write(srcDest, time);

                }
            }
        }
    }

    public static class IntSumCombiner
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();
        private Text words = new Text();

        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            float avg = sum / count;

            words.set(key.toString() + " " + count);

            result.set(Float.parseFloat(String.format("%.3f", avg)));
            context.write(words, result);
        }
    }

    // This is the Reducer class
    // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
    //
    // We want to control the output format to look at the following:
    //
    // count of word = count
    //
    public static class IntSumReducer
            extends Reducer<Text, FloatWritable, Text, Text> {

        private Text result_value = new Text();


        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }

            // generate result value
            result_value.set(String.valueOf(sum));

            context.write(key, result_value);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("mapred.textoutputformat.separator", " ");
        Logger.getRootLogger().setLevel(Level.WARN);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(AvergeTime.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumCombiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add the input paths as given by command line
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        // add the output path as given by the command line
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
