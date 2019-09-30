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

package spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*
* @packge: spark.streaming
* @program: Spark01_Stream
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-29 16:42
**/
object Spark01_Stream {
  def main(args: Array[String]): Unit = {

    // 初始化sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark01_Stream")

    // SparkContext对象
    // val sc = new SparkContext(sparkConf)

    // 采集周期，以指定的时间为周期进行采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    // 从指定的端口采集数据
    val socketline: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = socketline.flatMap {
      line => {
        line.split(" ")
      }
    }
    // 将数据改变结构
    val wordToOne: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 聚合操作
    val wordToSumDStream: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)

    // 将结果打印出来
    wordToSumDStream.print()

    // 不能停止采集程序
    // streamingContext.stop()

    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行
    streamingContext.awaitTermination()


  }
}
