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
package com.readailib.hadoop.chapter1.leftoutjoin.stage1;

import edu.umd.cloud9.io.pair.PairOfStrings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-09-03 16:14
 **/
public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, PairOfStrings,  PairOfStrings> {

    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    @Override
    /**
     * map()读取<transcation_id>,<product_id>,<user_id>,<quantity>,<amount>
     * 输出<user_id>,<product_id>
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        if (tokens.length == 2) {
            String userID = tokens[2];
            String productionID = tokens[1];
            outputKey.set(userID, "2");
            outputValue.set(productionID, "P");
            context.write(outputKey, outputValue);

        }

    }
}
