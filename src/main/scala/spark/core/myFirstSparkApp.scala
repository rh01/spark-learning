/*
 * Copyright 2018 @rh01 http://github.com/rh01
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
package spark.core

/*
* @packge: chapter1
* @program: myFirstSparkApp
* @description: ${description}
* @Author: Shen Hengheng 
* @create: 2018-09-03 20:38
**/

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object myFirstSparkApp{

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local")
      .appName("myFirstSpark20")
      .config("spark.spark.sql.warehouse.dir", ".")
      .getOrCreate()

    val x = Array(1.0,5.0,8.0,10.0,15.0,21.0,27.0,30.0,38.0,45.0,50.0,64.0)
    val y = Array(5.0,1.0,4.0,11.0,25.0,18.0,33.0,20.0,30.0,43.0,55.0,57.0)

    val xRDD = spark.sparkContext.parallelize(x)
    val yRDD = spark.sparkContext.parallelize(y)

    val zipedRDD = xRDD.zip(yRDD)
    zipedRDD.collect().foreach(println)

  }

}
