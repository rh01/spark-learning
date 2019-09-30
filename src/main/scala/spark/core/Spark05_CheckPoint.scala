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
object Spark05_CheckPoint {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")


    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    // 设置检查点的保存目录,一般情况下hdfs
    sc.setCheckpointDir("cp")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val mapRDD: RDD[(Int, Int)] = rdd.map((_,1))

    //mapRDD.checkpoint()

    val reduceRDD: RDD[(Int, Int)] = mapRDD.reduceByKey(_+_)
    //
    reduceRDD.checkpoint()


    reduceRDD.foreach(println)
    println(reduceRDD.toDebugString)
    // (4) ShuffledRDD[2] at reduceByKey at Spark05_CheckPoint.scala:42 []
    //  +-(4) MapPartitionsRDD[1] at map at Spark05_CheckPoint.scala:40 []
    //      |  ParallelCollectionRDD[0] at makeRDD at Spark05_CheckPoint.scala:38 []


    /**
      * (4) ShuffledRDD[2] at reduceByKey at Spark05_CheckPoint.scala:48 []
      *   +-(4) MapPartitionsRDD[1] at map at Spark05_CheckPoint.scala:44 []
      *       |  ReliableCheckpointRDD[3] at foreach at Spark05_CheckPoint.scala:50 []
      */


    /**
      * (4) ShuffledRDD[2] at reduceByKey at Spark05_CheckPoint.scala:48 []
      *  |  ReliableCheckpointRDD[3] at foreach at Spark05_CheckPoint.scala:52 []
      */
    // 释放
    sc.stop()
  }


}
