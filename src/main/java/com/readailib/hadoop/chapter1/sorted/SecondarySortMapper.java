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
package com.readailib.hadoop.chapter1.sorted;

import com.readailib.util.DateUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Date;

/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-09-01 11:49
 **/
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, NaturalValue> {

    /**
     * intialize key and value
     */
    private CompositeKey reduceKey = new CompositeKey();
    private NaturalValue reduceValue = new NaturalValue();


    @Override
    /**
     * @param key is hadoop generated, not used here
     * @param value a record of <stockSymbol><,><Date><,><price>
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = key.toString().split(",");
        if (tokens.length == 3) {
            // tokens[0] = stockSymbol;
            // tokens[1] = Date;
            // tokens[2] = price;
            Date date = DateUtil.getDate(tokens[1]);
            if (date == null) {
                return;
            }
            long timestamp = date.getTime();
            reduceKey.set(tokens[0], timestamp);
            reduceValue.set(timestamp, Integer.parseInt(tokens[2]));
            // // emit key-value pair
            context.write(reduceKey, reduceValue);
        } else {

        }

    }
}
