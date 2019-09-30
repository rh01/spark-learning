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
object Spark03_Op13 {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    // 从指定的数据集合中进行抽样处理，根据不同的算法进行抽样
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 5, 4, 3, 6, 7, 8, 1, 2))

    val mapRDD: RDD[(Int, Int)] = listRDD.map((_,1))

    val groupByKeyRDD: RDD[(Int, Iterable[Int])] = mapRDD.groupByKey()

    val finalRDD: RDD[Int] = groupByKeyRDD.map(t=>t._2.sum)

    finalRDD.collect().foreach(println)

  }
}
