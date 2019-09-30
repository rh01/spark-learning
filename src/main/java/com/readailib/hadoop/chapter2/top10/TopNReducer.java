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
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-10-23 10:45
 **/
public class TopNReducer  extends
        Reducer<NullWritable, Text, IntWritable, Text> {

    private int N = 10; // default
    private SortedMap<Integer, String> top = new TreeMap<Integer, String>();

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String valueAsString = value.toString().trim();
            String[] tokens = valueAsString.split(",");
            String url = tokens[0];
            int frequency =  Integer.parseInt(tokens[1]);
            top.put(frequency, url);
            // keep only top N
            if (top.size() > N) {
                top.remove(top.firstKey());
            }
        }

        // emit final top N
        List<Integer> keys = new ArrayList<Integer>(top.keySet());
        for(int i=keys.size()-1; i>=0; i--){
            context.write(new IntWritable(keys.get(i)), new Text(top.get(keys.get(i))));
        }
    }

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        this.N = context.getConfiguration().getInt("N", 10); // default is top 10
    }


}