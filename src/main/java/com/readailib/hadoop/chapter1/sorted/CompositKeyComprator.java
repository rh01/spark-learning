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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 用来比较两个Comprator
 * {@author Shen Hengheng}
 *
 * Comprator详细参考：
 * {@link com.readailib.hadoop.chapter1.sorted.CompositeKey}
 * @see <a href="http://www.baidu.com">baidu</a>
 */
public class CompositKeyComprator extends WritableComparator {


    protected CompositKeyComprator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable wk1, WritableComparable wk2) {
        CompositeKey ck1 = (CompositeKey) wk1;
        CompositeKey ck2 = (CompositeKey) wk2;

        int comparison = ck1.getStockSymbol().compareTo(ck2.getStockSymbol());
        if (comparison == 0) {
            if (ck1.getTimestamp() == ck2.getTimestamp()) {
                return 0;
            } else if (ck1.getTimestamp() > ck1.getTimestamp()) {
                return 1;
            } else {
                return -1;
            }
        } else {
            return comparison;
        }
    }


}
