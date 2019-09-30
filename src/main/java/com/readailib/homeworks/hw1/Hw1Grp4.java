package com.readailib.homeworks.hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/*
 * @program: hadoop
 * @description: HashBasedDistinct
 * @Author: shenhengheng
 * @create: 2018-04-01 08:18
 **/
public class Hw1Grp4 {
    public static void main(String[] args) throws IOException, InterruptedException {

        /** file path name*/
        String pathName;

        /** select column name*/
        Integer colNum;

        /** relation, such as gt,ge,eq,ne,le,lt etc.*/
        String relation;

        /** val*/
        Float number;

        /** list for store distinct column*/
        List<Integer> colList = new ArrayList<Integer>();

        /** Use Hash Table data structure to store data, and implement hash to distinct*/
        Hashtable<List<String>, String> hashtable = new Hashtable<List<String>, String>();

        /** WARN log level, filter logs*/
        Logger.getRootLogger().setLevel(Level.WARN);

        //1. check arguments whether correct.

        /** check argument number*/
        /** initial configuration object*/
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        /** 检查参数数目是否正确*/
        if (otherArgs.length < 3) {
            System.err.println("Usage: HashBasedDistinct R=<pathFile> select:R<colNum>,<relation>,<number> distinct:<col_list>");
            System.exit(2);
        }
        pathName = otherArgs[0].substring(otherArgs[0].indexOf("=") + 1);
        String selectString = otherArgs[1].substring(otherArgs[1].indexOf("R") + 1);
        List<String> selectStringList = Arrays.asList(selectString.split(","));
        colNum = Integer.parseInt(selectStringList.get(0));
        relation = selectStringList.get(1);
        number = Float.parseFloat(selectStringList.get(2));
        String distinctString = otherArgs[2].substring(otherArgs[2].indexOf(":") + 1);
        List<String> distinctList = Arrays.asList(distinctString.split(","));

        /** parse distinctList to Integer Array*/
        if (!distinctList.isEmpty()) {
            for (int i = 0; i < distinctList.size(); i++) {
                String substring = distinctList.get(i).substring(1);
                colList.add(Integer.parseInt(substring));
            }
        }

        //2. read fs/hdfs  file
        /** initial configuration object*/
        /** read file from fs or hdfs*/
        FileSystem fileSystem = FileSystem.get(URI.create(pathName), configuration);
        /** open file and put it in in_stream */
        FSDataInputStream in_stream = fileSystem.open(new Path(pathName));
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

        String s;
        while ((s = in.readLine()) != null) {
            /** remove spaces*/
            s = s.trim();
            try {
                String[] split = s.split("\\|");
                if (relation.equals("gt")) {
                    if (Float.parseFloat(split[colNum]) > number) {
                        List<String> stringList = new ArrayList<String>();
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("ge")) {
                    if (Float.parseFloat(split[colNum]) >= number) {
                        List<String> stringList = new ArrayList<String>();
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("eq")) {
                    if (Float.parseFloat(split[colNum]) == number) {
                        List<String> stringList = new ArrayList<String>();
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("ne")) {
                    if (Float.parseFloat(split[colNum]) != number) {
                        List<String> stringList = new ArrayList<String>();
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("lt")) {
                    if (Float.parseFloat(split[colNum]) < number) {
                        List<String> stringList = new ArrayList<String>();
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("le")) {
                    if (Float.parseFloat(split[colNum]) <= number) {
                        List<String> stringList = new ArrayList<String>();
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else {
                    System.out.println("wrong relation symbol!");
                    System.exit(7);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(8);
            }
        }

        //3. write data of hash table into hbase

        /** In HBase create tablename*/
        String tablename = "Result";
        /** configure hbase globally*/
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        /** HBaseAdmin manager HBase tables, create/delete/update/select etc*/
        HBaseAdmin admin = new HBaseAdmin(conf);
        /** check HBase whether existed "Result" table, if existed, delete!*/
        if (admin.tableExists(tablename)) {
            System.out.println("!!table exists! recreating.......!!");
            /** delete table procedure: disabled table -> delete table*/
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        /**  define column family*/
        HTableDescriptor htd =  new HTableDescriptor(tablename);
        HColumnDescriptor tcd = new HColumnDescriptor("res");
        /** create column family*/
        htd.addFamily(tcd);
        /** reate table*/
        admin.createTable(htd);
        admin.close();
        in.close();

        HTable table = new HTable(conf, tablename);
        Enumeration<List<String>> listEnumeration = hashtable.keys();
        int i = 0;
        while (listEnumeration.hasMoreElements()) {
            Put put = new Put(String.valueOf(i).getBytes());
            List<String> stringList = listEnumeration.nextElement();
            for (int j = 0; j < stringList.size(); j++) {
                put.add("res".getBytes(), distinctList.get(j).getBytes(), stringList.get(j).getBytes());
            }
            table.put(put);
            i += 1;
        }
        System.out.println(String.format("!!create table %s successfully...!!", tablename));
        table.close();
        fileSystem.close();
    }
}
