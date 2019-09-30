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
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-09-03 16:50
 **/
public class SecondarySortPartitioner extends Partitioner<PairOfStrings, Object> {
    @Override
    public int getPartition(PairOfStrings key, Object value, int numberOfPartitions) {
        return (key.getLeftElement().hashCode()) & Integer.MAX_VALUE % numberOfPartitions;
    }
}
