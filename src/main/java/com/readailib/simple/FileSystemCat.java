package com.readailib.simple;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/*
 * @program: hadoop
 * @description: 打印出hdfs存储的文件,读取hadoop文件系统中的/test/fstab文件
 * @Author: ReadAILib
 * @create: 2018-03-29 19:31
 **/
public class FileSystemCat {
    public static void main(String[] args) throws Exception {
        /** 目标文件系统的地址*/
        String uri="hdfs://192.168.59.150:8020/hw1/customer.tbl";
        /** 实例化一个Configuration对象 */
        Configuration configuration=new Configuration();
        /** URI接受String对象，返回一个URI对象 */
        FileSystem fileSystem=FileSystem.get(URI.create(uri), configuration);


        /** 数据流入 */
        FSDataInputStream in=null;
        in=fileSystem.open(new Path(uri));

        /** 将读取到hdfs内容复制到系统的输出设备中，然后利用输出设备输出文件的内容，即执行out.write(buf, 0, bytesRead);*/



        IOUtils.copyBytes(in, System.out, 4096, false);
        IOUtils.closeStream(in);
    }
}
