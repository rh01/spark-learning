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

//import javafx.util.Pair;
import edu.umd.cloud9.io.pair.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.umd.cloud9.io.pair.PairOfStrings;

import java.io.IOException;

/*
 * @program: hadoop
 * @description: 用户信息
 * @Author: Shen Hengheng
 * @create: 2018-09-03 16:14
 **/
public class LeftJoinUserMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();

    @Override
    /**
     * map()读取（user_id, location_id）,发出一个键（user_id, location_id）
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] tokens = value.toString().split("\t");
        if (tokens.length == 2) {
            String userID = tokens[0];
            String locationID = tokens[1];
            outputKey.set(userID, "1");
            outputValue.set("L",locationID);
            context.write(outputKey, outputValue);

        }

    }
}
