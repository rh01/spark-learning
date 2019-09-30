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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-08-31 15:30
 **/
public class NaturalValue implements Writable, WritableComparable<NaturalValue> {


    private long timestamp;
    private double price;

    public static NaturalValue  copy(NaturalValue  value) {
        return new NaturalValue(value.timestamp, value.price);
    }

    public NaturalValue(long timestamp, double price) {
        set(timestamp, price);
    }

    public NaturalValue() {
    }

    public void set(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getPrice() {
        return this.price;
    }

    /**
     * Deserializes the point from the underlying data.
     * @param in a DataInput object to read the point from.
     */
    public void readFields(DataInput in) throws IOException {
        this.timestamp  = in.readLong();
        this.price  = in.readDouble();
    }

    /**
     * Convert a binary data into NaturalValue
     *
     * @param in A DataInput object to read from.
     * @return A NaturalValue object
     * @throws IOException
     */
    public static NaturalValue read(DataInput in) throws IOException {
        NaturalValue value = new NaturalValue();
        value.readFields(in);
        return value;
    }

    public String getDate() {
        return DateUtil.getDateAsString(this.timestamp);
    }

    /**
     * Creates a clone of this object
     */
    public NaturalValue clone() {
        return new NaturalValue(timestamp, price);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.timestamp);
        out.writeDouble(this.price);

    }

    /**
     * Used in sorting the data in the reducer
     */
    @Override
    public int compareTo(NaturalValue data) {
        if (this.timestamp  < data.timestamp ) {
            return -1;
        }
        else if (this.timestamp  > data.timestamp ) {
            return 1;
        }
        else {
            return 0;
        }
    }

}
