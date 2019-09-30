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
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/*
* @packge: spark.sql
* @program: Spark02_SQL
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-29 08:19
**/
object Spark04_UDF {
  def main(args: Array[String]): Unit = {

    // 初始化SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark03_Transform")

    // 初始化SparkContext对象
    // 初始化SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // 引入隐式转换，方便rdd与DF，DS对象的转换
    import spark.implicits._

    // 自定义聚合函数
    // 创建聚合函数对象
    val udaf = new MyAgeAvgFunction
    spark.udf.register("avgAge", udaf)

    val frame: DataFrame = spark.read.json("in/user.json")

    frame.show()

    frame.createOrReplaceTempView("user")

    spark.sql("select avgAge(age) from user").show()

    // 释放ss对象
    spark.close()


  }
}

// 声明用户自定义聚合函数
// 1 继承UserDefinedAggregateFunction
// 2 实现方法
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  // 输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 计算时的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 函数返回时的数据结构
  override def dataType: DataType = {
    DoubleType
  }

  // 函数是否为稳定
  override def deterministic: Boolean = {
    true
  }

  // 计算之前的缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询的结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    // count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算，最终的结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1).toDouble
  }
}
