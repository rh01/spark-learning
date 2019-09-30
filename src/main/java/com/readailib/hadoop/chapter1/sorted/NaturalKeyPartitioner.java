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

import org.apache.hadoop.mapreduce.Partitioner;

//import static java.util.Objects.hash;

/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-08-31 15:29
 **/
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, NaturalValue> {
    @Override
    public int getPartition(CompositeKey compositeKey, NaturalValue naturalValue, int numberOfPartitions) {
        return Math.abs((int) (hash(compositeKey.getStockSymbol()) % numberOfPartitions));
    }

    /**
     *  adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31*h + str.charAt(i);
        }
        return h;
    }
}
