package com.readailib.homeworks.hw1;

import lombok.extern.slf4j.Slf4j;
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
import org.apache.hadoop.hbase.io.hfile.bucket.BucketAllocator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

/**
 * @Description: HashJoin的实现
 * 要求：java Hw1GrpX R=<file 1> S=<file 2> join:R<col1>=S<col2> res:R<col3>,S<col4>
 * @Author: ReadAILib
 * @Date: 2018/4/2
 */
@Slf4j
public class HashJoin {


    public static void main(String[] args) throws IOException, InterruptedException {


        /** 定义文件路径名字*/
        String fileName1;
        String fileName2;


        /** 定义文件的要Join的第几列（Key）*/
        Integer joinKey1;
        Integer joinKey2;


        /**定义文件要Join的值（Value）*/
        Integer joinValue1;
        Integer joinValue2;

        String relation;

        /** 定义约束值*/
        Float number;

        /** 使用Hash Table来存储对象，并哈希运算*/
        //Hashtable<List<String>, String> hashtable = new Hashtable<List<String>, String>();

        //1. 检查参数格式是否正确

        /** 检查参数数目是否正确*/
        if (args.length < 4) {
            log.error("参数不正确！");
            System.out.println("Usage: HashJoin R=<file 1> S=<file 2> join:R<col1>=S<col2> res:R<col3>,S<col4>");
            System.exit(2);
        }
        /** 第一个文件的路径*/
        if (!args[0].startsWith("R=")) {
            log.error("第一个参数不正确，参数格式为R=<file 1> ");
            System.out.println("第一个参数格式不正确");
            System.exit(3);
        }
        fileName1 = args[0].substring(args[0].indexOf("=") + 1);

        /** 第二个文件的路径*/
        if (!args[1].startsWith("S=")) {
            log.error("第二个参数不正确，参数格式为S=<file 1> ");
            System.out.println("第二个参数格式不正确");
            System.exit(3);
        }
        fileName2 = args[0].substring(args[1].indexOf("=") + 1);

        /** 需要join的join key*/
        if (!args[2].startsWith("join:")) {
            log.error("第三个参数不正确，参数格式为join:R<col1>=S<col2> ");
            System.out.println("第三个参数格式不正确");
            System.exit(3);
        }
        joinKey1 = Integer.parseInt(args[2].substring(args[2].indexOf("R") + 1,
                args[2].indexOf("=")));

        joinKey2 = Integer.parseInt(args[2].substring(args[2].indexOf("S") + 1));


        /** 需要join的value*/
        if (!args[3].startsWith("res:")) {
            log.error("第四个参数不正确，参数格式为res:R<col3>,S<col4> ");
            System.out.println("第四个参数格式不正确");
            System.exit(3);
        }
        joinValue1 = Integer.parseInt(args[3].substring(args[3].indexOf("R") + 1,
                args[3].indexOf(",")));

        joinValue2 = Integer.parseInt(args[3].substring(args[3].indexOf("S") + 1));


        /** 打印参数列表*/
        log.info(Arrays.toString(args));


        //2. 读取文件系统的文件
        /** 初始化一个配置对象*/
        Configuration configuration = new Configuration();
        /** 从指定的文件系统中获得相应的文件*/
        FileSystem fileSystem1 = FileSystem.get(URI.create(fileName1), configuration);
        FileSystem fileSystem2 = FileSystem.get(URI.create(fileName2), configuration);

        /** 数据流入 */
        FSDataInputStream in_stream1 = fileSystem1.open(new Path(fileName1));
        FSDataInputStream in_stream2 = fileSystem2.open(new Path(fileName2));


        /** 利用hadoop来读取*/
        BufferedReader in1 = new BufferedReader(new InputStreamReader(in_stream1));
        BufferedReader in2 = new BufferedReader(new InputStreamReader(in_stream2));
        String s1;
        String s2;


        // TODO: 2018/4/2 实现simple hashjoin操作
        /** 定义三个hashmap的变量*/
        Map<String, ArrayList<String[]>> hashTable1 = new HashMap<String, ArrayList<String[]>>();

        Map<String, ArrayList<String[]>> hashTable2 = new HashMap<String, ArrayList<String[]>>();
        Map<String, ArrayList<String[]>> resultTable = new HashMap<String, ArrayList<String[]>>();


        /** 读取第一个文件，并对指定的joinkey hash*/
        while ((s1 = in1.readLine()) != null) {
            ArrayList<String[]> arrayList = new ArrayList<String[]>();
            String[] strings = new String[2];

            s1 = s1.trim();
            String[] record = s1.split("\\|");
            /** 去除join key和对应的join的value值*/
            String record_1 = record[joinKey1];
            String record_2 = record[joinValue1];
            /** 将这两个值添加进去*/
            strings[0] = record_1;
            strings[1] = record_2;
            //arrayList.add(record_1);
            //arrayList.add(record_2)
            //System.out.println(strings[0] + "|" + strings[1]);
            ;


            /** 计算hash值*/
            String id = mkHash(record_1);
            /** 如果hash表中存在key为id值的key，就将他们加进去*/
            if (hashTable1.containsKey(id)) {
                hashTable1.get(id).add(strings);
            } else {
                arrayList.add(strings);
                hashTable1.put(id, arrayList);
            }




        }

        Set<String> strings1 = hashTable1.keySet();
        Iterator<String> iterator = strings1.iterator();

        for (String s : strings1) {

            System.out.println(s + "|" + Arrays.toString(hashTable1.get(s).get(5)) + "|" + Arrays.toString(hashTable1.get(s).get(6))+"|");

        }

        /** 读取第二个文件，进行hash*/
        while ((s2 = in2.readLine()) != null) {
            ArrayList<String[]> arrayList2 = new ArrayList<String[]>();
            String[] strings2 = new String[2];

            s2 = s2.trim();
            String[] record2 = s2.split("\\|");
            /** 去除join key和对应的join的value值*/
            String record_1 = record2[joinKey2];
            String record_2 = record2[joinValue2];
            /** 将这两个值添加进去*/
            strings2[0] = record_1;
            strings2[1] = record_2;
            //arrayList.add(record_1);
            //arrayList.add(record_2);


            /** 计算hash值*/
            String id = mkHash(record_1);
            /** 如果hash表中存在key为id值的key，就将他们加进去*/
            if (hashTable2.containsKey(id)) {
                hashTable2.get(id).add(strings2);
            } else {
                arrayList2.add(strings2);
                hashTable2.put(id, arrayList2);
            }

        }

        Set<String> strings2 = hashTable2.keySet();
        Iterator<String> iterator2 = strings2.iterator();

//        for (String s : strings2) {
//
//            System.out.println(s + "|" + Arrays.toString(hashTable2.get(s).get(5)) + "|" + Arrays.toString(hashTable2.get(s).get(6))+"|");
//
//        }

        // TODO: 2018/4/2 下面实现S表对其进行哈希操作


        ArrayList<String[]> result;
        result = new ArrayList<String[]>();
        System.out.println("第一个hash表的大小为" + hashTable1.size());
        System.out.println("第二个hash表的大小为" + hashTable2.size());
        int i = 0;
        for (String teste : hashTable1.keySet()) {
            //System.out.println(teste);
            if (hashTable2.containsKey(teste)) {
                for (String[] record1 : hashTable1.get(teste)) {

//                    System.out.println(record1.length);
//                    for (int k =0 ; k<record1.length; k++) {
//                        System.out.println(record1[k]);
//                    }
                    //System.out.println(record1[0]+", " + record1[1]);
                    for (String[] record2 : hashTable2.get(teste)) {




                        //System.out.println("record2:" + record2.length);
//                    for (int k =0 ; k<record2.length; k++) {
//                        System.out.println(record2[k]);
//                    }
                        if (record1[0].equals(record2[0])) {
                            String[] strings = new String[3];
                            strings[0] = record1[0];
                            strings[1] = record1[1];
                            strings[2] = record2[1];
                            result.add(strings);
                            //System.out.println("|" + strings[0] + "|" + strings[1] + "|" + strings[2] + "|");
                        }
//                        // result.add(JoinData(record1,record2));
////                        System.out.println(Arrays.toString(result.get(i)));
//                        //	System.out.println(Arrays.toString(JoinColumn(table1.getColumnName(),table2.getColumnName())));
//                        i++;
//                        //System.out.println(Arrays.toString(JoinData(record.getData(), record2.getData())));
                    }

                }
            }


        }

//        for (int j = 0; j < result.size(); j++) {
//            String[] strings = result.get(j);
//            System.out.println("|" + strings[0] + "|" + strings[1] + "|" + strings[2] + "|");
//        }
        log.info("{}",result.size());





        //3. HashTable中的数据写入HBase中


        /** 这个将会在HBase中设置为表的名字*/
        String tablename = "Result";
        /** 对HBase进行配置，其中set方法接受(String,String)，然后通过set方法为属性设置值*/
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hw");
        //conf.set("hbase.master", "master:60000");
        /** HBaseAdmin提供了一个接口来管理HBase数据库的表信息，比如增删改查，使得表无效有效，删除或者增加列族成员*/
        HBaseAdmin admin = new HBaseAdmin(conf);
        /** 判断数据库中是否存在表名为wordcount的表，如果存在则删除该表！*/
        if (admin.tableExists(tablename)) {
            System.out.println("table exists! recreating.......");
            /** 调用HBase DML的API，使得该表无效（disable）并删除（drop）*/
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        /** HTableDescriptor包含了表的名字以及对应表的列族。 */
        HTableDescriptor htd = new HTableDescriptor(tablename);
        /** 列族为content
         /** HColumnDescriptor 维护着关于列族的信息，例如版本号，压缩设置等。*/
        /** 它通常在创建表或者为表添加列族的时候使用。列族被创建后不能直接修改，只能通过删除然后重新创建的方式。*/
        /** 列族被删除的时候，列族里面的数据也会同时被删除*/
        HColumnDescriptor tcd = new HColumnDescriptor("res");
        /** 创建列族*/
        htd.addFamily(tcd);
        /** 创建表*/
        admin.createTable(htd);
        admin.close();
        /** Distinct */
        //

        /** 将数据写到 HBase 中*/
        in1.close();
        in2.close();

        /** 创建Htable句柄 */
        HTable table = new HTable(conf, tablename);

        //遍历key
        //Enumeration<List<String>> listEnumeration = hashtable.keys();
        //int i = 0;
        for (int k = 0; k < result.size(); k++) {

            Put put = new Put(result.get(k)[0].getBytes());
            put.add("res".getBytes(), result.get(k)[1].getBytes(), null);
            put.add("res".getBytes(), result.get(k)[2].getBytes(), null);
            table.put(put);

        }

        /** 测试hashtable的大小*/
        log.info("HBase 的Row数: {} ", i);
        table.close();
        fileSystem1.close();
        fileSystem2.close();
    }

    public static String mkHash(String key) {
        Integer hashValue = key.hashCode();
        Integer bucktNum = hashValue % 101; //这里设置hash范围为0-10
        String chave = String.valueOf(bucktNum);
        return chave;
    }
}
