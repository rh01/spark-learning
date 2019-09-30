package com.readailib.hbase.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/*
 * @program: hadoop
 * @description:
 * @Author: ReadAILib
 * @create: 2018-03-30 23:32
 **/
public class WordCountHbaseReaderReduce extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text val:values) {
            result.set(val);
            context.write(key, result);
        }
    }
}
