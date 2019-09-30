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
package com.readailib.hadoop.chapter1.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * @program: hadoop
 * @description: reducer
 * @Author: ReadAILib
 * @create: 2018-08-31 09:02
 **/
public class SecondarySortingTemperatureReducer extends Reducer<DateTemperaturePair, Text, Text, Text>{

    @Override
    /**
     * @param key generated from mapper's out
     * @param values
     *
     * @author ShenHengheng
     */
    protected void reduce(DateTemperaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text value : values) {
            builder.append(value.toString());
            builder.append(",");
        }

        // 写结果或者hdfs
        context.write(key.getYearMonth(), new Text(builder.toString()));

    }
}
