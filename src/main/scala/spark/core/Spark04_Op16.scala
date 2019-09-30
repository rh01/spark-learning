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

import org.apache.spark.{SparkConf, SparkContext}

/*
* @packge: chapter1
* @program: Spark01_RDD
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-28 09:25
**/
object Spark04_Op16 {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    // 总结：reduceByKey与GroupByKey的区别
    // reduceByKey： 按照key来聚合，在shuffle之前有combine聚合操作，返回结果为RDD[k,v]
    // groupByKey: 按照key进行分组，直接shuffle
    val listRDD = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 1)))


    listRDD.saveAsTextFile("output1")
    listRDD.saveAsObjectFile("output2")
    listRDD.saveAsSequenceFile("output3")
    // listRDD.foldByKey()
  }
}
