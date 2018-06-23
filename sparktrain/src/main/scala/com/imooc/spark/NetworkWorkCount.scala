package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming处理socket数据
  */
object NetworkWorkCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWorkCount")


    /**
      * 创建StreamingContext需要两个参数：SparkConf和batch interval
      */
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val lines = ssc.socketTextStream("192.168.95.131", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)


    result.print()

    ssc.start()
    ssc.awaitTermination()

  }


}
