package com.readailib.hbase.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map.Entry;

/*
 * @program: hadoop
 * @description:
 * @Author: ReadAILib
 * @create: 2018-03-30 23:22
 **/
public class WordCountHbaseReaderMapper extends TableMapper<Text, Text>{
    public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer("");
        for (Entry<byte[], byte[]> entry : value.getFamilyMap("content".getBytes()).entrySet()) {
            String str = new String(entry.getValue());
            /** 将字节数组转换为String类型*/
            if (str != null) {
                stringBuffer.append(new String(entry.getKey()));
                stringBuffer.append(";");
                stringBuffer.append(str);
            }
            context.write(new Text(key.get()), new Text(new String(stringBuffer)));
        }
    }
}
