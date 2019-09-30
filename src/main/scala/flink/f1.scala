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

package flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/*
* @packge: flink
* @program: f1
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-27 19:50
**/
object f1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)


//    val text = env.readTextFilele("file:///F:\\06_workshop\\0820\\big-data-hbase-master\\src\\main\\scala\\flink\\test1.txt")
//
//    val wordCounts = text
//      .flatMap{ _.split(" ") map { (_, 1) } }
//      .keyBy(0)
//      .timeWindow(Time.seconds(5))
//      .sum(1)
//    wordCounts.print()
//
//    env.execute("Word Count Example")


    //      readTextFile("file://F:\\06_workshop\\0820\\big-data-hbase-master\\src\\main\\scala\\flink\\test1.txt")
//      .print()
//    val text:DataStream[String] = env
//      .readTextFile("file://F:\\06_workshop\\0820\\big-data-hbase-master\\src\\main\\scala\\flink\\test1.txt")

//    val mapped = text.map { x => x.toInt}
//    text.print()

  }
}
