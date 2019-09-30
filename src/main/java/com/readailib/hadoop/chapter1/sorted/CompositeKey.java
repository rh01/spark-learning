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

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Input format:
 * <p>
 * |Stock-Symbol |    Date    |Closed-Price|
 * | ILMN        | 2013-12-05 |    97.65   |
 * | ILMN        | 2013-12-05 |    97.65   |
 * | ILMN        | 2013-12-05 |    97.65   |
 * | ILMN        | 2013-12-05 |    97.65   |
 * <p>
 * Output format:
 * <p>
 * ILMN: (2013-12-05,23) (2013-12-06,34.03)
 * IBM:  (2013-12-04,13) (2013-12-07,31.03) (2013-12-09,30.3)
 * <p>
 * <p>
 * __________________________________________________________________________________
 * |                        |                            |                           |
 * |        Stock Symbol    |          timestamp         |           Price           |
 * |________________________|____________________________|___________________________|
 * |<----------------------><-------------------------------------------------------->
 * 自然键                                           自然值
 * </--------------------------------------------------->
 * 组合键
 *
 *
 * @author ShenHengheng
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    private String stockSymbol; // 股票代码
    private long timestamp; // 日期

    public CompositeKey(String stockSymbol, long timestamp) {
       set(stockSymbol, timestamp);
    }

    public CompositeKey() {
    }

    public void set(String stockSymbol, long timestamp) {
        this.stockSymbol = stockSymbol;
        this.timestamp = timestamp;
    }

    public String getStockSymbol() {
        return stockSymbol;
    }

    public void setStockSymbol(String stockSymbol) {
        this.stockSymbol = stockSymbol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public int compareTo(CompositeKey other) {
        if (this.stockSymbol.compareTo(other.stockSymbol) != 0) {
            return this.stockSymbol.compareTo(other.stockSymbol);
        } else if (this.timestamp != other.timestamp) {
            return this.timestamp < other.timestamp ? -1 : 1;
        }else {
            return 0; // 两个实例相等
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.stockSymbol);
        out.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.stockSymbol = in.readUTF();
        this.timestamp = in.readLong();
    }
}
