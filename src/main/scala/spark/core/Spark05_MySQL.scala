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

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

/*
* @packge: chapter1
* @program: Spark01_RDD
* @description:  ${description}
* @Author: Shen Hengheng 
* @create: 2019-09-28 09:25
**/
object Spark05_MySQL {
  def main(args: Array[String]): Unit = {
    // local模式
    // 创建spark配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

    // 创建spark上下文环境
    val sc = new SparkContext(conf)

    //2.创建一个RDD
    // val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.59.150:3306/rdd"
    val userName = "root"
    val passWd = "Heng-130509"

    //创建JdbcRDD
    /*  val rdd = new JdbcRDD(sc, () => {
        //获取数据库连接对象
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
        "select name, age from user where id>=? and id <=?",
        1, // lowerBound
        3, // upperBound
        2,
        (rs) => {
          println(rs.getString(1) + "," + rs.getInt(2))
        }
      )

      rdd.collect()*/


    // 保存数据
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zhangsan", 22), ("wangwu", 34), ("lisi", 34)))


    /* 这种做法很容易导致创建胡connection过多
    dataRDD.foreach{
      case (username, age) =>{
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)

        val spark.sql = "insert into user (name, age) values(?,?)"
        val statement: PreparedStatement = connection.prepareStatement(spark.sql)

        statement.setString(1,username)
        statement.setInt(2, age)
        statement.executeUpdate()

        statement.close()
        connection.close()
      }
    }
    */

    // 改进，缺点容易出现OOM
    dataRDD.foreachPartition( datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)


      datas.foreach{
        case (username, age) => {
          val sql = "insert into user (name, age) values(?,?)"
          val statement: PreparedStatement = connection.prepareStatement(sql)

          statement.setString(1,username)
          statement.setInt(2, age)
          statement.executeUpdate()

          statement.close()
        }
      }

      connection.close()
    })

    sc.stop()
  }


}

