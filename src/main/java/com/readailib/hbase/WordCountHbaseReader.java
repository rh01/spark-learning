package com.readailib.hbase;

import com.readailib.hbase.mapper.WordCountHbaseReaderMapper;
import com.readailib.hbase.reducer.WordCountHbaseReaderReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/*
 * @program: hadoop
 * @description:
 * @Author: ReadAILib
 * @create: 2018-03-30 23:35
 **/
public class WordCountHbaseReader {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tablename = "wordcount1";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "master,dn1,dn2,dn3");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: WordCountHbaseReader <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "WordCountHbaseReader");
        job.setJarByClass(WordCountHbaseReader.class);
        //设置任务数据的输出路径；
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        job.setReducerClass(WordCountHbaseReaderReduce.class);
        Scan scan = new Scan();
        TableMapReduceUtil.initTableMapperJob(tablename,scan,WordCountHbaseReaderMapper.class, Text.class, Text.class, job);
        //调用job.waitForCompletion(true) 执行任务，执行成功后退出；
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
