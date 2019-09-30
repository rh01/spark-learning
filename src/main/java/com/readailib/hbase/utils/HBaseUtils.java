package com.readailib.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;


import java.io.IOException;
import java.util.Map;

/*
 * @program: hadoop
 * @description: HBase API
 * @Author: ReadAILib
 * @create: 2018-03-30 23:05
 **/
public class HBaseUtils {
    public static void scan(String tableName) {
        /** new Configuration();已经废弃*/
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "Master");

        HTable table = null;
        ResultScanner rs = null;

        try {
            Scan scan = new Scan();
            table = new HTable(configuration, tableName);
            rs = table.getScanner(scan);
            for (Result row : rs) {
                System.out.format("ROW\t%s\n", new String());
                for (Map.Entry<byte[], byte[]> entry : row.getFamilyMap("info".getBytes()).entrySet()) {
                    String column = new String(entry.getKey());
                    String value = new String(entry.getValue());
                    System.out.format("COLUMN\t info:%s\t%s\n", column, value);
                }

                for (Map.Entry<byte[], byte[]> entry : row.getFamilyMap("address".getBytes()).entrySet()) {
                    String column = new String(entry.getKey());
                    String value = new String(entry.getValue());
                    System.out.format("COLUMN\t address:%s\t%s\n", column, value);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
