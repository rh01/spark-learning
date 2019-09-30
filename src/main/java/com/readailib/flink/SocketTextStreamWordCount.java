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
package com.readailib.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Date 2019/09/27
 *
 * @author: Shen Hengheng
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * Solution:
 * </p>
 **/
public class SocketTextStreamWordCount {

    /**
     * Implements the string tokenizer that splits sentences into words as
     * a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it in
     * to
     * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&
     * gt;).
     */
    public static  class LineSplitter implements
            FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>
                > out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("USAGE: \\nSocketTextStreamWordCount <hostname> <p\n" +
                    "ort>");
            return;
        }

        String hostName = args[0];
        int hostPort = Integer.parseInt(args[1]);

        // 设置本地的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取数据
        DataStream<String> text = env.socketTextStream(hostName, hostPort, '\n', 1024);
        text.flatMap(new LineSplitter()).setParallelism(1)
                // group by the tuple field "0" and sum up tuple field "1"
                .keyBy(0)
                .sum(1).setParallelism(1)
                .print();


        // // execute program
        env.execute("Java WordCount from SocketTextStream Example");

    }

}


