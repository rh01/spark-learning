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

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
* @packge: chapter1
* @program: Spark01_RDD
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-28 09:25
**/
object Spark06_HBase {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(sparkConf)

    //2.创建一个RDD
    // val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    val conf = HBaseConfiguration.create()
//    conf.set(TableInputFormat.INPUT_TABLE, "student")
//
//    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
//      conf,
//      classOf[TableInputFormat],
//      classOf[ImmutableBytesWritable],
//      classOf[Result]
//    )
//
//    hbaseRDD.foreach{
//      case (rk, res) => {
//        val cells: Array[Cell] = res.rawCells()
//        for (cell <- cells) {
//          println(Bytes.toString(CellUtil.cloneValue(cell)))
//        }
//      }
//    }


    
    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1002", "zhangsan"), ("1003", "lisi"), ("1004", "wamhwu")))

    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rk, name) => {

        val put = new Put(Bytes.toBytes(rk))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))

        (new ImmutableBytesWritable(Bytes.toBytes(rk)), put)
      }
    }

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")


    putRDD.saveAsHadoopDataset(jobConf)



    sc.stop()
  }


}

