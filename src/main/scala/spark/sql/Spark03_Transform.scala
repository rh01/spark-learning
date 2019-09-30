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

package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
* @packge: spark.sql
* @program: Spark02_SQL
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-29 08:19
**/
object Spark03_Transform {
  def main(args: Array[String]): Unit = {

    // 初始化SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_Transform")

    // 初始化SparkContext对象
    // 初始化SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 引入隐式转换，方便rdd与DF，DS对象的转换
    import spark.implicits._

    // 创建RDD
    val dataRDD: RDD[(String, String, String)] = spark.sparkContext.makeRDD(
      List(("1", "shenheng", "23"), ("2", "liming", "33"), ("3", "zhangwu", "43"))
    )

    // 转化为DF
    val frame: DataFrame = dataRDD.toDF("id", "name", "age")

    // frame.show()
    // 转化为DS
    val ds: Dataset[People] = frame.as[People]

    // 转化为DF
    val df1: DataFrame = ds.toDF()

    // 转化为rdd
    val rdd: RDD[Row] = df1.rdd

    // 获取数据：可以通过索引来访问
    rdd.foreach {
      case row =>{
        println(row.getString(1))
      }
    }

    // RDD-to-DS
    val peopleRDD: RDD[People] = dataRDD.map {
      case (id, name, age) => {
        People(id, name, age)
      }
    }

    val peopleds: Dataset[People] = peopleRDD.toDS()

    val rdd2: RDD[People] = peopleds.rdd
    rdd2.foreach(println)

    // 释放ss对象
    spark.close()


  }
}

case class People(id: String, name:String, age:String)
