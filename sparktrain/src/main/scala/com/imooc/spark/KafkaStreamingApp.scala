package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka
  */
object KafkaStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      System.err.print("Usage: KafkaReceiverWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //TODO... Spark Streaming如何对接Kafka
    val message = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
    //message.print()

    //TODO... 自己测试为什么要取第二个
    //message.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    message.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()

  }
}
