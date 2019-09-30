package com.readailib.hbase.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * @program: hadoop
 * @description:
 * @Author: ReadAILib
 * @create: 2018-03-30 21:31
 **/
public class WordCountHbaseMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreElements()) {
            word.set(tokenizer.nextToken());
            /** 输出<key,value>为<word,one>*/
            context.write(word, one);
        }
    }
}
