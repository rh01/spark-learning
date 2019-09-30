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

package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
* @packge: spark.sql
* @program: Spark01
* @description: ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-29 08:10
**/
object Spark01_Demo {
  def main(args: Array[String]): Unit = {
    // 初始化SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01")

    // 初始化SparkContext对象
    // 初始化SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val frame: DataFrame = spark.read.json("in/temp.json")

    frame.show()

    // 释放sparksession对象
    spark.close()
  }
}
