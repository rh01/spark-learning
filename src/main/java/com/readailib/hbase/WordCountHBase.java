package com.readailib.hbase;

import com.readailib.hbase.mapper.*;

import com.readailib.hbase.reducer.WordCountHbaseReaderReduce;
import com.readailib.hbase.reducer.WordCountHbaseReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/*
 * @program: hadoop
 * @description:
 * @Author: ReadAILib
 * @create: 2018-03-30 21:31
 **/
public class WordCountHBase {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /** 这个将会在HBase中设置为表的名字*/
        String tablename = "wordcount";
        /** 对HBase进行配置，其中set方法接受(String,String)，然后通过set方法为属性设置值*/
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hw");
        //conf.set("hbase.master", "master:60000");
        /** HBaseAdmin提供了一个接口来管理HBase数据库的表信息，比如增删改查，使得表无效有效，删除或者增加列族成员*/
        HBaseAdmin admin = new HBaseAdmin(conf);
        /** 判断数据库中是否存在表名为wordcount的表，如果存在则删除该表！*/
        if(admin.tableExists(tablename)){
            System.out.println("table exists! recreating.......");
            /** 调用HBase DML的API，使得该表无效（disable）并删除（drop）*/
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        /** HTableDescriptor包含了表的名字以及对应表的列族。 */
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));
        /** 列族为content*/
        /** HColumnDescriptor 维护着关于列族的信息，例如版本号，压缩设置等。*/
        /** 它通常在创建表或者为表添加列族的时候使用。列族被创建后不能直接修改，只能通过删除然后重新创建的方式。*/
        /** 列族被删除的时候，列族里面的数据也会同时被删除*/
        HColumnDescriptor tcd = new HColumnDescriptor("content");
        /** 创建列族*/
        htd.addFamily(tcd);
        /** 创建表*/
        admin.createTable(htd);

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: WordCountHbase <in>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "WordCountHbase");
        job.setJarByClass(WordCountHBase.class);
        /** 使用WordCountHbaseMapper类完成Map过程；*/
        job.setMapperClass(WordCountHbaseMapper.class);
        /** job.setCombinerClass(WordCountHbaseReducer.class); */
        TableMapReduceUtil.initTableReducerJob(tablename, WordCountHbaseReducer.class, job);
        /** 设置任务数据的输入路径；*/
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        /**设置了Map过程和Reduce过程的输出类型，其中设置key的输出类型为Text；*/
        job.setOutputKeyClass(Text.class);
        /**设置了Map过程和Reduce过程的输出类型，其中设置value的输出类型为IntWritable；*/
        job.setOutputValueClass(IntWritable.class);
        /** 调用job.waitForCompletion(true) 执行任务，执行成功后退出；*/
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
