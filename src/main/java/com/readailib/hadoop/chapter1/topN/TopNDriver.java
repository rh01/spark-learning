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
package com.readailib.hadoop.chapter1.topN;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/*
 * @program: hadoop
 * @description: spark
 * @Author: Shen Hengheng
 * @create: 2018-09-01 12:54
 **/
public class TopNDriver {
    public static void main(String[] args) {

        // 1. [处理命令行参数]
        if (args.length < 1) {
            System.out.println("Usage: Top10 <hdfs-file>");
            System.exit(1);
        }


        String inputPath = args[0];
        System.out.println("Input Path: <hdfs-file>=" + inputPath);

        // 2. [[连接Spark master]]
        SparkConf conf = new SparkConf().setAppName("TopNDriver").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(conf);

        // 3. [[从HDFS上读取文件]]
        JavaRDD<String> lines = ctx.textFile(inputPath);

        // 4. [[创建一组]]
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] tokens = line.split(","); // cat24, 123

                return new Tuple2<String, Integer>(tokens[0], Integer.parseInt(tokens[1]));
            }
        });

        // 5. [[为每个分区创建一个本地top10列表]]
        JavaRDD<SortedMap<Integer, String>> partitions = pairs.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Integer>>, SortedMap<Integer, String>>() {
            @Override
            public Iterator<SortedMap<Integer, String>> call(Iterator<Tuple2<String, Integer>> iter) throws Exception {
                SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
                while (iter.hasNext()) {
                    Tuple2<String, Integer> tuple = iter.next();
                    // tuple._1 : cat_id
                    // tuple._2 : cat_weight
                    top10.put(tuple._2, tuple._1);
                    if (top10.size() > 10) {
                        top10.remove(top10.firstKey());
                    }
                }
                return Collections.singletonList(top10).iterator();
            }
        });

        // 6. [[使用collect创建最终的top10列表]]
//        SortedMap<Integer, String> finalTop10 = new TreeMap<Integer, String>();
//        List<SortedMap<Integer, String>> alltop10 = partitions.collect();
//        for (SortedMap<Integer, String> localtop10 : alltop10) {
//            // weight = tuple._1
//            // cat = tuple._2
//            for (Map.Entry<Integer, String> entry : localtop10.entrySet()) {
//                System.out.println(entry.getKey() + "--" + entry.getValue());
//                finalTop10.put(entry.getKey(), entry.getValue());
//                if (finalTop10.size() > 10) {
//                    finalTop10.remove(finalTop10.firstKey());
//                }
//            }
//        }

        // 6. [[使用reduce]]
        SortedMap<Integer, String> finalTop10 = partitions.reduce(new Function2<SortedMap<Integer, String>, SortedMap<Integer, String>, SortedMap<Integer, String>>() {
            @Override
            public SortedMap<Integer, String> call(SortedMap<Integer, String> v1, SortedMap<Integer, String> v2) throws Exception {
                SortedMap<Integer, String> top10 = new TreeMap<Integer, String>();
                // 处理v1
                for (Map.Entry<Integer, String> entry : v1.entrySet()) {
                    top10.put(entry.getKey(), entry.getValue());
                    if (top10.size() > 10) {
                        top10.remove(top10.firstKey());
                    }
                }

                // 处理v2
                for (Map.Entry<Integer, String> entry : v2.entrySet()) {
                    top10.put(entry.getKey(), entry.getValue());
                    if (top10.size() > 10) {
                        top10.remove(top10.firstKey());
                    }
                }

                return top10;
            }
        });


        // 7. [[输出最终的top10]]
        Set<Map.Entry<Integer, String>> entries = finalTop10.entrySet();
        for (Map.Entry<Integer, String> entry : entries) {
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }

    }
}
