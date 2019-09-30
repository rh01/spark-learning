package com.readailib.hbase.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * @program: hadoop
 * @description:
 * @Author: ReadAILib
 * @create: 2018-03-30 21:37
 **/
public class WordCountHbaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //put实例化，每一个词存一行
        Put put = new Put(key.getBytes());
        //列族为content,列修饰符为count，列值为数目
        put.add(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
        context.write(new ImmutableBytesWritable(key.getBytes()), put);// 输出求和后的<key,value>

    }
}
