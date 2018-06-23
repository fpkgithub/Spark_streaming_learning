package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理HDFS文件数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //    val lines = ssc.textFileStream("file:///home/hadoop/data/imooc/")
    val lines = ssc.textFileStream("file:///D:/IDE/Idea/sparktrain/data/imooc")
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
