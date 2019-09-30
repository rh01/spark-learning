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
* @program: WordCount
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-28 08:22
**/
object WordCount {
  def main(args: Array[String]): Unit = {
    
    // 使用开发工具完成Spark WordCount的开发

    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    // 读取文件，将文件内容一行一行的读取出来
    val lines: RDD[String] = sc.textFile("F:\\06_workshop\\0820\\big-data-hbase-master\\src\\main\\scala\\chapter1\\test1.txt")

    // flatmap，将一行一行的数据分解成一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 为了统计方便，将单词数据进行结构的转换
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))

    // 将相同单词的数据进行分组聚合
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    // 采集后打印到控制台上
    val result: Array[(String, Int)] = wordToSum.collect()
    result.foreach(println)

    // 停止
    sc.stop()

  }

}
