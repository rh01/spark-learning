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
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;

/*
 * @program: hadoop
 * @description: 如何按照自然键分组
 * @Author: Shen Hengheng
 * @create: 2018-09-03 16:51
 **/
public class SecondarySortGroupComparator implements RawComparator<PairOfStrings> {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        DataInputBuffer buffer = new DataInputBuffer();
        PairOfStrings a = new PairOfStrings();
        PairOfStrings b = new PairOfStrings();
        try {
            buffer.reset(b1, s1, l1);
            a.readFields(buffer);
            buffer.reset(b2, s2, l2);
            b.readFields(buffer);
            return compare(a, b);
        } catch (Exception ex) {
            return -1;
        }
    }


    @Override
    /**
     *  Group only by userID
     */
    public int compare(PairOfStrings first, PairOfStrings second) {
        return first.getLeftElement().compareTo(second.getLeftElement());
    }
}
