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
 * @description: Select的实现
 *               java Hw1GrpX R=<file> select:R1,gt,5.1
 *               a. 输入文件：<file>
 *                   例如：/hw1/lineitem.tbl等
 *               b. 选择: 只有一列，数值列
 *                   6种形式(a)列,gt,值, (b)列,ge,值, (c)列,eq,值, (d)列,ne,值,(e)列,le,值, (f)列,lt,值
 *                   涵义：> gt; >= ge; == eq; != ne; le <=; lt <
 *                   例如：R的第1列大于5.1 (从第0列数起)
 * @Author: ReadAILib
 * @create: 2018-04-01 08:18
 **/

public class Hw1Grp41 {


    public static void main(String[] args) throws IOException {


        /** 文件路径名字*/
        String pathName;

        /** 定义文件的第几列*/
        int colNum;

        /**定义关系*/
        String relation;

        /** 定义约束值*/
        float number;

        /** 使用Hash Table来存储对象，并哈希运算*/
        /** 使用Hash Table来存储对象，内部哈希运算，这里也可以使用HashMap，HashSet等数据结构来实现distinct*/
        Hashtable<List<String>, String> hashtable = new Hashtable<List<String>, String>();

        // Step 1. 检查参数格式是否正确

        //GenericOptionsParser.printGenericCommandUsage();
        Configuration configuration = new Configuration();
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        /** 检查参数数目是否正确*/
        if (otherArgs.length < 3) {
            System.err.println("Usage: HashBasedDistinct R=<pathFile> select:R<colNum>,<relation>,<number> distinct:<col_list>");
            System.exit(2);
        }


        /** 目标文件的路径*/
        if (!args[0].startsWith("R=")) {
            rootLogger.error("第一个参数不正确，参数格式为R=file");
            System.out.println("第一个参数格式不正确");
            System.exit(3);
        }
        pathName = args[0].substring(args[0].indexOf("=") + 1);

        /** R的第几列，输出出来*/
        if (!args[1].startsWith("select:R")) {
            rootLogger.error("第二个参数格式不正确");
            System.out.println("第二参数不争取也=，应该时select:col_num");
            System.exit(4);
        }

        String selectString = args[1].substring(args[1].indexOf("R") + 1);
        List<String> selectStringList = Arrays.asList(selectString.split(","));
        colNum = Integer.parseInt(selectStringList.get(0));
        /** R列与列之间的关系*/
        relation = selectStringList.get(1);
        /** 约束值*/
        number = Float.parseFloat(selectStringList.get(2));

        /** distinct*/
        if (!args[2].startsWith("distinct")) {
            rootLogger.error("第四个参数格式不正确");
            System.out.println("第四参数不争取也=，应该时distinct:<distinct_list>");
            System.exit(5);
        }
        String distinctString = args[2].substring(args[2].indexOf(":") + 1);
        List<String> distinctList = Arrays.asList(distinctString.split(","));

        /** 打印参数列表*/
        rootLogger.info(Arrays.toString(args));




        // Step 2. 读取文件系统的文件
        /** 初始化一个配置对象*/
        //Configuration configuration = new Configuration();
        /** 从指定的文件系统中获得相应的文件*/
        FileSystem fileSystem = FileSystem.get(URI.create(pathName), configuration);
        /** 数据流入 */
        FSDataInputStream in_stream = null;
        in_stream = fileSystem.open(new Path(pathName));

        /** 利用hadoop来读取*/
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;

        // TODO: 处理多个distinct列
        //int col1 = Integer.parseInt(distinctList.get(0).substring(1));
        //int col2 = Integer.parseInt(distinctList.get(1).substring(1));
        //int col3 = Integer.parseInt(distinctList.get(2).substring(1));
        //System.out.println(distinctList);

        /** 定义一个列表*/
        List<Integer> colList = new ArrayList<Integer>();

        if (!distinctList.isEmpty()) {
            for (int i = 0; i < distinctList.size(); i++) {
                String substring = distinctList.get(i).substring(1);
                colList.add(Integer.parseInt(substring));
            }

        }


        int sum = 0;


        while ((s = in.readLine()) != null) {
            /*去除两端的空格*/
            s = s.trim();
            try {
                // 将s进行处理,按照分隔符将每一行分割成数组
                String[] split = s.split("\\|");

                // 检查对应的列是否与指定的数字相同
                if (relation.equals("gt")) {
                    if (Float.parseFloat(split[colNum]) > number) {
                        sum += 1;

                        List<String> stringList = new ArrayList<String>();
                        /** 将数据存放在hashtable中*/
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("ge")) {
                    if (Float.parseFloat(split[colNum]) >= number) {
                        sum += 1;
                        /** 将数据存放在hashtable中*/
                        List<String> stringList = new ArrayList<String>();
                        /** 将数据存放在hashtable中*/
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("eq")) {

                    if (Float.parseFloat(split[colNum]) == number) {
                        sum += 1;
                        /** 将数据存放在hashtable中*/
                        List<String> stringList = new ArrayList<String>();
                        /** 将数据存放在hashtable中*/
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("ne")) {
                    if (Float.parseFloat(split[colNum]) != number) {
                        sum += 1;
                        List<String> stringList = new ArrayList<String>();
                        /** 将数据存放在hashtable中*/
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("lt")) {
                    if (Float.parseFloat(split[colNum]) < number) {
                        sum += 1;
                        /** 将数据存放在hashtable中*/
                        List<String> stringList = new ArrayList<String>();
                        /** 将数据存放在hashtable中*/
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else if (relation.equals("le")) {
                    if (Float.parseFloat(split[colNum]) <= number) {
                        sum += 1;
                        /** 将数据存放在hashtable中*/
                        List<String> stringList = new ArrayList<String>();
                        /** 将数据存放在hashtable中*/
                        for (int i = 0; i < colList.size(); i++) {
                            stringList.add(split[colList.get(i)]);
                        }
                        hashtable.put(stringList, "");
                    }
                } else {
                    System.out.println("关系运算符错误！");
                    System.exit(7);
                }


            } catch (Exception e) {
                System.out.println("文件格式出粗或者处理方式出错");
                System.exit(8);
                //System.out.println(e);
            }
        }
        rootLogger.info("真实的数据大小：" + sum);


        // Step 3. HashTable中的数据写入HBase中




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
        // TODO： distinct实现

        /** 将数据写到 HBase 中*/
        // TODO：将处理完的数据写如HBase中in.close();

        /** 创建Htable句柄 */
        HTable table = new HTable(conf, tablename);

        //遍历key
        Enumeration<List<String>> listEnumeration = hashtable.keys();
        int i = 0;
        while (listEnumeration.hasMoreElements()) {

            Put put = new Put(String.valueOf(i).getBytes());
            List<String> stringList = listEnumeration.nextElement();

            for (int j = 0; j< stringList.size(); j++) {
                put.add("res".getBytes(), stringList.get(j).getBytes(), null);
            }



            table.put(put);
            i += 1;
        }
        /** 测试hashtable的大小*/
        rootLogger.info("HBase 的Row数: " + i) ;
        table.close();
        fileSystem.close();
    }


}
