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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import javax.xml.soap.Text;
import java.io.IOException;

/*
 * @program: hadoop
 * @description: reduce program
 * @Author: Shen Hengheng
 * @create: 2018-09-01 11:59
 **/
public class SecondarySortReduer extends Reducer<CompositeKey, NaturalValue, Text, Text> {

    @Override
    protected void reduce(CompositeKey key, Iterable<NaturalValue> values, Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (NaturalValue data : values) {
            builder.append("(");
            String dateAsString = DateUtil.getDateAsString(data.getTimestamp());
            double price = data.getPrice();
            builder.append(dateAsString);
            builder.append(",");
            builder.append(price);
            builder.append(")");
        }
        context.write(new Text(key.getStockSymbol()), new Text(builder.toString()));
    }
}
