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
package com.readailib.util;

/*
 * @program: hadoop
 * @description:
 * @Author: Shen Hengheng
 * @create: 2018-09-01 11:32
 **/


import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class provide a basic Date utility functions.
 *
 * @author Mahmoud Parsian
 *
 */
public class DateUtil {

    static final String DATE_FORMAT = "yyyy-MM-dd";
    static final SimpleDateFormat SIMPLE_DATE_FORMAT =
            new SimpleDateFormat(DATE_FORMAT);

    /**
     *  Returns the Date from a given dateAsString
     */
    public static Date getDate(String dateAsString)  {
        try {
            return SIMPLE_DATE_FORMAT.parse(dateAsString);
        }
        catch(Exception e) {
            return null;
        }
    }

    /**
     *  Returns the number of milliseconds since January 1, 1970,
     *  00:00:00 GMT represented by this Date object.
     */
    public static long getDateAsMilliSeconds(Date date) throws Exception {
        return date.getTime();
    }


    /**
     *  Returns the number of milliseconds since January 1, 1970,
     *  00:00:00 GMT represented by this Date object.
     */
    public static long getDateAsMilliSeconds(String dateAsString) throws Exception {
        Date date = getDate(dateAsString);
        return date.getTime();
    }




    public static String getDateAsString(long timestamp) {
        return SIMPLE_DATE_FORMAT.format(timestamp);
    }

}
