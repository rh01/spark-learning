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
object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    // 创建RDD
    // 1) 从内存中创建RDD, 底层实现就是parallize
    // val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 使用自定义分区
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 2) 从内存创建RDD
    // val arrayRDD: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4))

    // 3）从外存创建RDD
    // 默认情况下，可以读取项目路径，也可以读取其他路径：HDFS等
    // 默认从文件中读取恶的数据都是字符串类型
    // 读取文件时，传递的分区参数为最小分区书，但是不一定是这个分区书，取决于hadoop的读取文件的分区
    // 规则
    val fileRDD: RDD[String] = sc.textFile("F:\\06_workshop\\0820\\big-data-hbase-master\\src\\main\\scala\\chapter1\\test1.txt", 3)

    // listRDD.collect().foreach(println)

    // 将RDD的数据保存在文件中
    fileRDD.saveAsTextFile("output")


  }
}
