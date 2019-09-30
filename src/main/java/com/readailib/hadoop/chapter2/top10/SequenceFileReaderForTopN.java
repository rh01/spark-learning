/*
 * Copyright 2018 @rh01 https://github.com/rh01
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.readailib.hadoop.chapter2.top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.net.URI;
import java.util.Random;

/**
 * This is a driver class, which creates a sample
 * SequenceFile of (K: Text, V: Integer) pairs.
 *
 * This is for demo/testing purposes.
 *
 * @author Mahmoud Parsian
 *
 */
public class SequenceFileReaderForTopN {

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IOException("usage: java org.dataalgorithms.chap03.mapreduce.SequenceFileWriterForTopN <hdfs-path> <number-of-entries>");
        }

        final String uri = args[0];                 // HDFS path like: /topn/input/sample.seq

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
        //
        IntWritable key = new IntWritable();
        Text value = new Text();
        final SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,conf);

        try {
            while (reader.next(key, value)) {
                System.out.println(key.get() + "\t" + value.toString());
            }
        }
        finally {
            IOUtils.closeStream(reader);
        }
    }
}