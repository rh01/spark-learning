package com.readailib.simple;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/*
 * @program: hadoop
 * @description: 简单的单词计数程序
 * @Author: ReadAILib
 * @create: 2018-03-29 19:55
 **/
public class WordCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable();
        private Text word = new Text();

        /**
         * 实现Mapper的方法
         */
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while (stringTokenizer.hasMoreElements()) {
                word.set(stringTokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    /**
     * Reduce层，主要实现的单词计数的reduce过程,Reduce类以map的输出作为输入,因此它的输入类型为map的输出类型
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }


    /**
     * @Description: main方法，主要实现业务逻辑
     * @Param: [args]
     * @return: void
     * @Author: ReadAILib
     * @Date: 2018/3/29
     */
    public static void main(String[] args) throws Exception {

        JobClient client = new JobClient();
        /** job的初始化过程*/
        JobConf jobConf = new JobConf(WordCount.class);
        /** 命名job,以便JobTacker和TaskTracker进行监视 */
        jobConf.setJobName("wordcount");

        /** 设置key和value的数据类型，其中Text相当于java的String类型，IntWritable相当于java的int类型*/
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        /** 这个是我自定义的Map和Reduce类，然后实现了具体的逻辑*/
        /** 设置job的处理三种过程，Map、Combine和Reduce*/
        jobConf.setMapperClass(Map.class);
        jobConf.setCombinerClass(Reduce.class);
        jobConf.setReducerClass(Reduce.class);

        /** 接着设置输入和输出路径*/
        /** 生成InputSplit的方法可以通过InputFormat()来设置。InputFormat()方法是用来生成可供map处理的<key,value>对的。*/
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        //JobClient jobClient = new JobClient();
        client.setConf(jobConf);

        try{
            JobClient.runJob(jobConf);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
