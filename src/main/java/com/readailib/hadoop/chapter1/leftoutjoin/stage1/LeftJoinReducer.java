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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import scala.util.Left;
import scala.util.Right;

import java.io.IOException;
import java.util.Iterator;

/*
 * @program: hadoop
 * @description: 左连接规约器
 * @Author: Shen Hengheng
 * @create: 2018-09-03 16:48
 **/
public class LeftJoinReducer extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
    Text productID = new Text();
    Text locationID = new Text("undefined");

    @Override
    protected void reduce(PairOfStrings key, Iterable<PairOfStrings> values, Context context) throws IOException, InterruptedException {
        Iterator<PairOfStrings> iterator = values.iterator();
        if (iterator.hasNext()) {
            // firstPair must be location pair
            PairOfStrings firstPair = iterator.next();
            System.out.println("firstPair=" + firstPair.toString());
            if (firstPair.getLeftElement().equals("L")) {
                locationID.set(firstPair.getRightElement());
            }
        }

        while (iterator.hasNext()) {
            // the remaining elements must be product pair
            PairOfStrings productPair = iterator.next();
            System.out.println("productPair=" + productPair.toString());
            productID.set(productPair.getRightElement());
            context.write(productID, locationID);
        }
    }
}

