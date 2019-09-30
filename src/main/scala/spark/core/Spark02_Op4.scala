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

package spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* @packge: chapter1
* @program: Spark01_RDD
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-28 09:25
**/
object Spark02_Op4 {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    // map算子
    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    // mapPartion可以对一个RDD中所有的分区进行遍历
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datasets => datasets)

    flatMapRDD.foreach(println)


  }
}
