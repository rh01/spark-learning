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

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/*
* @packge: chapter1
* @program: Spark01_RDD
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-28 09:25
**/
object Spark08_Accumator {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    // TODO 创建累加器
    val wordAccumulator = new WordAccumulator

    // TODO 注册累加器
    sc.register(wordAccumulator)

    rdd.foreach{
      case  word => {
        // TODO 执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }

    // TODO 获取累加器的值
    println(wordAccumulator.value)

    sc.stop()
  }


}

// 生命累加器
// 1. 继承父类[in，out]
// 2. 实现抽象方法
// 3. 创建累加器
class WordAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {
  val list = new util.ArrayList[String]()

  // 当前的累加器是否为初始化状态
  override def isZero: Boolean = list.isEmpty

  // 复制累加器
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordAccumulator()
  }

  // 充值累加器
  override def reset(): Unit = list.clear()

  // 像累加器中增加数据
  override def add(v: String): Unit = {
    if (v.contains("h")){
      list.add(v)
    }
  }

  // 合并累加器
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  // 获取累加器的结果
  override def value: util.ArrayList[String] = list
}

