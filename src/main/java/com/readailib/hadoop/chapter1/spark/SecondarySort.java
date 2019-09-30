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
package com.readailib.hadoop.chapter1.spark;

//import com.kenai.jaffl.annotations.In;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
//import org.jruby.compiler.ir.Tuple;
import scala.Int;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

/*
 * @program: hadoop
 * @description: spark version
 * @author:Shen Hengheng
 * @create: 2018-08-31 10:10
 **/
public class SecondarySort {

    /**
     * 比较器
     */
    public static class TupleComparator implements Comparator<Tuple2<Integer, Integer>>,Serializable {
        @Override
        public int compare(Tuple2<Integer, Integer> t1 , Tuple2<Integer, Integer> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static void main(String[] args) {
        // 1. [[验证参数]]
        if (args.length < 1) {
            System.out.println("Usage: secondarySort <file>");
            System.exit(1);
        }
        String inputPath = args[0];
        System.out.println("args[0]: <file>=" + args[0]);

        // 连接Spark Master
        // 2. [[通过创建一个JavaSparkContext对象，连接到Spark Context]]
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        final JavaSparkContext ctx = new JavaSparkContext(conf);

        // 3. [[创建SparkRDD]]
        // format: <name><,><time><,><value>
        JavaRDD<String> lines = ctx.textFile(inputPath, 1);

        // 4. [[从JavaRDD创建pairRDD]]
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer, Integer>> call(String line) throws Exception {
                String[] tokens = line.split(","); // x, 2, 5
                System.out.println(tokens[0] + "," + tokens[1] + "," + tokens[2]);
                Integer time = new Integer(tokens[1]);
                Integer value = new Integer(tokens[2]);
                Tuple2<Integer, Integer> timevalue = new Tuple2<Integer, Integer>(time, value);
                return new Tuple2<String, Tuple2<Integer, Integer>>(tokens[0], timevalue);
            }
        });

        // 5. [[验证pairRDD]]
        List<Tuple2<String, Tuple2<Integer, Integer>>> output = pairs.collect();
        for (Tuple2 t : output) {
            Tuple2<Integer, Integer> timevalue = (Tuple2<Integer, Integer>) t._2;
            System.out.println(t._1 + "," + timevalue._1 + "," + timevalue._2);
        }

        // 6. [[按键来分组]]
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> groups = pairs.groupByKey();

        // 7. [[验证]]
        System.out.println("===Debug1===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output2 = groups.collect();
        for (Tuple2 t : output2) {
            System.out.println(t._1);
            Iterable<Tuple2<Integer, Integer>> list = (Iterable<Tuple2<Integer, Integer>>) t._2;
            for (Tuple2<Integer, Integer> li : list) {
                System.out.println(li._1 + "," + li._2);
            }
            System.out.println("=======");
        }


        // 8. [[排序]]
        // mapValues[U](f: JFunction[V, U]): JavaPairRDD[K, U]
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> sorted =
                groups.mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>>() {
                    @Override
                    public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, Integer>> s) throws Exception {
                        List<Tuple2<Integer, Integer>> newList = new ArrayList<Tuple2<Integer, Integer>>(iterableToList(s));
                        Collections.sort(newList, new TupleComparator());
                        return newList;
                    }
                });

        // 9. [[输出最终结果]]
        System.out.println("===Debug2===");
        List<Tuple2<String, Iterable<Tuple2<Integer, Integer>>>> output3 = sorted.collect();
        for (Tuple2 t : output3) {
            System.out.println(t._1);
            Iterable<Tuple2<Integer, Integer>> list = (Iterable<Tuple2<Integer, Integer>>) t._2;
            for (Tuple2<Integer, Integer> li : list) {
                System.out.println(li._1 + "," + li._2);
            }
            System.out.println("=======");
        }

        // 10. [[write to hdfs]]
        sorted.saveAsTextFile("hdfs://master:8020/test/sorted");
    }

    /**
     * 辅助函数：用来将iterable转换为List类型
     * @param iterable
     * @return List
     */
    static List<Tuple2<Integer,Integer>> iterableToList(Iterable<Tuple2<Integer,Integer>> iterable) {
        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer,Integer>>();
        for (Tuple2<Integer,Integer> item : iterable) {
            list.add(item);
        }
        return list;
    }


}
