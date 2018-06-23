# 第10章 Spark Streaming整合Kafka

本章节将讲解Spark Streaming整合Kafka的两种方式，讲解如何在本地进行开发测试，如何在服务器上进行测试 

[TOC]

##10-1 -课程目录

- 实战一：Receive-based
- 实战二：Direct Approach



##10-2 -Spark Streaming整合Kafka的版本选择详解

Spark Streaming + Kafka Integration Guide

http://spark.apache.org/docs/latest/streaming-kafka-integration.html



Spark Streaming + Kafka Integration Guide (Kafka broker version 0.8.2.1 or higher)

http://spark.apache.org/docs/latest/streaming-kafka-0-8-integration.html#spark-streaming-kafka-integration-guide-kafka-broker-version-082



##10-3 -Receiver方式整合之概述

**Write Ahead Logs**

预写日志机制：在Spark1.2后引进，To ensure zero-data loss, you have to additionally enable Write Ahead Logs in Spark Streaming (introduced in Spark 1.2). 



##10-4 -Receiver方式整合之Kafka测试

Receiver整合
1) 启动zk
2) 启动kafka
3) 创建topic
4) 通过控制台测试本topic是否能够正常的生产和消费信息



启动Zookeeper

```shell
zkServer.sh start
```

启动Kafka

```shell
##以后台进程的方式启动
kafka-server-start.sh -daemon /home/hadoop/app/kafka_2.11-0.9.0.0/config/server.properties
```

创建topic：boy_kafka_streaming_topic

```shell
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic boy_kafka_streaming_topic

kafka-topics.sh --list --zookeeper localhost:2181
```

通过控制台测试本topic是否能够正常的生产和消费信息

```shell
##消费者 指定broker
kafka-console-producer.sh --broker-list localhost:9092 --topic boy_kafka_streaming_topic

##生产者 指定zookeeper
kafka-console-consumer.sh --zookeeper localhost:2181 --topic boy_kafka_streaming_topic
```



##10-5 -Receiver方式整合之Spark Streaming应用开发

依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
    <version>${spark.version}</version>
</dependency>
```



```scala
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka的方式一
  */
object KafkaReceiverWordCount {

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
    message.print()

    //TODO... 自己测试为什么要取第二个
    message.map(_._2).flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
```

Program arguments 

```shell
hadoop000 test boy_kafka_streaming_topic 1
```



##10-6 -Receiver方式整合之本地环境联调

打包 上传



提交

```shell
spark-submit \
--class com.imooc.spark.KafkaReceiverWordCount \
--master local[2] \
--name KafkaReceiverWordCount \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
/home/hadoop/lib/sparktrain-1.0.jar  hadoop000:2181 test kafka_streaming_topic 1


spark-submit --class com.imooc.spark.KafkaReceiverWordCount --master local[2] --name KafkaReceiverWordCount --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 /home/hadoop/lib/boy/sparktrain-1.0.jar hadoop000 test boy_kafka_streaming_topic 1
```



##10-7 -Receiver方式整合之服务器环境联调及Streaming UI讲解

Web UI：192.168.95.131:4040



##10-8 -Direct方式整合之概述

**Approach 2: Direct Approach**

http://spark.apache.org/docs/latest/streaming-kafka-0-8-integration.html#approach-2-direct-approach-no-receivers

新特性：

*Simplified Parallelism:* 

*Efficiency:*  

*Exactly-once semantics:*  

使用

```scala
KafkaUtils.createDirectStream[
     [key class], [value class], [key decoder class], [value decoder class] ](
     streamingContext, [map of Kafka parameters], [set of topics to consume])
```



##10-9 -Direct方式整合之Spark Streaming应用开发及本地环境测试

```scala
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder

/**
  * Spark Streaming对接Kafka的方式二
  */
object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.print("Usage: KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    //TODO... Spark Streaming如何对接Kafka
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet
    )
    messages.print()

    //TODO... 自己测试为什么要取第二个
    messages.map(_._2).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
```

郁闷郁闷，偶遇编辑器错误，无法自动导入import kafka.serializer.StringDecoder，手动导入会出错如下：

```scala
Spark Streaming 报错:kafka.cluster.BrokerEndPoint cannot be cast to kafka.cluster.Broker
```

解决思路：

注解掉kafka依赖

```xml
<!-- &lt;!&ndash; Kafka 依赖  &ndash;&gt;
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka_2.11</artifactId>
    <version>${kafka.version}</version>
</dependency>-->
```



##10-10 -Direct方式整合之服务器环境联调

```shell
##启动Zookeeper
zkServer.sh start

##以后台进程的方式启动kafka
kafka-server-start.sh -daemon /home/hadoop/app/kafka_2.11-0.9.0.0/config/server.properties

##创建topic
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic boy_kafka_streaming_topic

##查看topic
kafka-topics.sh --list --zookeeper localhost:2181

##消费者 指定broker
kafka-console-producer.sh --broker-list localhost:9092 --topic boy_kafka_streaming_topic

##生产者 指定zookeeper
kafka-console-consumer.sh --zookeeper localhost:2181 --topic boy_kafka_streaming_topic

##提交
spark-submit --class com.imooc.spark.KafkaDirectWordCount --master local[2] --name KafkaDirectWordCount --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 /home/hadoop/lib/boy/sparktrain-1.0.jar  hadoop000:9092 boy_kafka_streaming_topic
```



---

Boy-20180620

