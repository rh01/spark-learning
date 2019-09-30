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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;


/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-10-23 10:45
 **/
public class TopNMapper extends Mapper<Text,IntWritable,NullWritable,Text> {

    private SortedMap<Integer, String> top10Cat = new TreeMap<Integer, String>();
    private int N = 10;

    /**
     * 每个映射器第一次运行时执行的代码，只运行一次
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.N = configuration.getInt("N",10);

    }

    /**
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String keyAsString = key.toString();
        int frequency =  value.get();
        String compositeValue = keyAsString + "," + frequency;

        top10Cat.put(frequency, compositeValue);
        if (top10Cat.size() > N) {
            top10Cat.remove(top10Cat.firstKey());
        }
    }


    /**
     * 每个映射器最后执行的函数
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String catAttribute : top10Cat.values()) {
            context.write(NullWritable.get(), new Text(catAttribute));
        }
    }
}