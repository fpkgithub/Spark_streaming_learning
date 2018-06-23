package com.imooc.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
  * 累计求和功能:
  * 在插入数据前先判断单词是否存在，如果存在就update，不存在则insert
  */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 如果使用了stateful的算子，必须设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("hadoop000", 6789)
    val result = lines.flatMap(_.split(" ").map((_, 1))).reduceByKey(_ + _)

    //state.print()  //此处仅仅是将统计结果输出到控制台

    //TODO...  将结果写入到MySQL
    /*result.foreachRDD(rdd => {
      val connection = createConnection()
      rdd.foreach { record =>
        val sql = "insert into wordcount(word,wordcount) values('" + record._1 + "'," + record._2 + ")"
        connection.createStatement().execute(sql)
      }
    })*/
    result.print()

    result.foreachRDD(rdd => {

      rdd.foreachPartition(partitionOfRecords => {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {

          //先查询
          val sqlFind = "select word,wordcount from wordcount where word=" + "'" + record._1 + "'"
          val results = connection.createStatement().executeQuery(sqlFind)
          if (results.next()) {
            val sqlUpdate = "update wordcount set wordcount = wordcount +" + record._2 + " where word = '" + record._1 + "'"
            print("sqlUpate:" + sqlUpdate)
            connection.createStatement().execute(sqlUpdate)
          } else {
            val sql = "insert into wordcount(word,wordcount) values('" + record._1 + "'," + record._2 + ")"
            print("sql:" + sql)
            connection.createStatement().execute(sql)
          }
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 获取Mysql的连接
    *
    * @return
    */
  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark", "root", "root")
  }

}
