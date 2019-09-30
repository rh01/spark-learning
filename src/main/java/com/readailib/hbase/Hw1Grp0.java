package com.readailib.hbase;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;


import org.apache.log4j.*;


@Slf4j
public class Hw1Grp0 {

    public static void main(String[] args) throws IOException, URISyntaxException {
        String fileR = args[0].substring(2);
        String fileS = args[1].substring(2);
        int RIndex = args[2].charAt(6) - '0';
        int SIndex = args[2].charAt(9) - '0';
        String[] columnResult = args[3].substring(4).split(",");
        log.info(Arrays.toString(args));
        String tableName = "Result";
        String columnFamily = "res";

        Hashtable<Integer, ArrayList<String[]>> R = readHDFS(fileR, RIndex);
        Hashtable<Integer, ArrayList<String[]>> S = readHDFS(fileS, SIndex);

        Hashtable<String, ArrayList<String[]>> resultTable = hashJoin(R, RIndex, S, SIndex, columnResult);

        createTable(tableName, columnFamily, resultTable);

    }

    /*
     * read HDFS, create hash table
     * @param file File name
     * @param index Index of join key
     * @return Hash table
     */
    public static Hashtable<Integer, ArrayList<String[]>> readHDFS(String file, int index) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        Hashtable<Integer, ArrayList<String[]>> table = new Hashtable<Integer, ArrayList<String[]>>();
        while ((s = in.readLine()) != null) {
            String[] split = s.split("\\|");
            String joinkey = split[index];
            int id = joinkey.hashCode();
            if (table.containsKey(id))
                table.get(id).add(split);
            else {
                ArrayList<String[]> record = new ArrayList<String[]>();
                record.add(split);
                table.put(id, record);
            }
        }

        in.close();

        fs.close();

        return table;
    }

    /*
     * join two table using hash join
     * @param R Hash table of table R
     * @param RIndex The index of join key of table R
     * @param S hash Table of table S
     * @param SIndex The index of join key of table S
     * @param columnResult The array of result's column
     */
    public static Hashtable<String, ArrayList<String[]>> hashJoin(Hashtable<Integer, ArrayList<String[]>> R, int RIndex, Hashtable<Integer, ArrayList<String[]>> S, int SIndex, String[] columnResult) {
        Hashtable<String, ArrayList<String[]>> table = new Hashtable<String, ArrayList<String[]>>();
        for (int id : R.keySet()) {
            if (S.containsKey(id)) {
                for (String[] recordR : R.get(id)) {
                    for (String[] recordS : S.get(id)) {
                        if (recordR[RIndex].compareTo(recordS[SIndex]) == 0) {
                            String key = recordR[RIndex];
                            if (table.containsKey(key))
                                table.get(key).add(joinData(recordR, recordS, columnResult));
                            else {
                                ArrayList<String[]> record = new ArrayList<String[]>();
                                record.add(joinData(recordR, recordS, columnResult));
                                table.put(key, record);
                            }
                        }
                    }

                }
            }
        }
        return table;
    }

    /*
     * @param recordR A record of table R
     * @param recordS A record of table S
     * @param key Join key of R and S
     * @param columnResult Set of column of R and S in join result
     */
    public static String[] joinData(String[] recordR, String[] recordS, String[] columnResult) {
        String[] unionRecord = new String[2 * columnResult.length];

        for (int i = 0; i < columnResult.length; ++i) {
            unionRecord[i] = columnResult[i];
            if (columnResult[i].charAt(0) == 'R')
                unionRecord[i + columnResult.length] = recordR[columnResult[i].charAt(1) - '0'];
            else
                unionRecord[i + columnResult.length] = recordS[columnResult[i].charAt(1) - '0'];
        }
        return unionRecord;
    }

    /*
     * create HBase table
     * @param tableName Name of HBase table
     * @param columnFamily Name of column family of tableName
     */
    public static void createTable(String tableName, String columnFamily, Hashtable<String, ArrayList<String[]>> ht) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {

        Logger.getRootLogger().setLevel(Level.WARN);

        String tablename = "Result";

        // create table descriptor
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));

        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor(columnFamily);
        htd.addFamily(cf);

        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hw");
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);

        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println(tableName + " is exist,detele....");
        }

        hAdmin.createTable(htd);
        hAdmin.close();
        System.out.println("table " + tableName + " created successfully");

        for (String str : ht.keySet()) {
            Put put = new Put(str.getBytes());
            ArrayList<String[]> record = ht.get(str);

            int length = record.get(0).length / 2;
            for (int i = 0; i < length; ++i) {
                put.add(columnFamily.getBytes(), record.get(0)[i].getBytes(), record.get(0)[i + length].getBytes());
            }
            for (int i = 1; i < record.size(); ++i) {
                for (int j = 0; j < length; ++j) {
                    put.add(columnFamily.getBytes(), (record.get(i)[j].getBytes() + "." + Integer.toString(i)).getBytes(), record.get(i)[j + length].getBytes());
                }
            }
            System.out.println("Insert Successfully");
        }

    }


    /*
     * Hash string to int
     * @param str String to be hashed
     * @return Hash value of str
     */
    public static int myHashFun(String str) {
        int id = str.hashCode() % 101;
        return id;
    }

}
