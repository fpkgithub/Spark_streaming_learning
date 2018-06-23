# 第11章 Spark Streaming整合Flume&Kafka打造通用流处理基础

本章节将通过实战案例彻底打通Spark Streaming和Flume以及Kafka的综合使用，为后续项目实战打下坚实的基础 

## 11-1 -课程目录

- 整合日志输出到Flume

- 整合Flume到Kafka

- 整和Kafka到Spark Streaming

- Spark Streaming对接收到的数据进行处理

  

## 11-2 -处理流程画图剖析

**整体架构图**

![整体架构图](https://upload-images.jianshu.io/upload_images/5959612-5f6c2cff347bee67.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



## 11-3 -日志产生器开发并结合log4j完成日志的输出

**模拟日志产生**

```java
import org.apache.log4j.Logger;

/**
 * 模拟日志产生
 */

public class LoggerGenerator
{

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException
    {
        int index = 0;
        while (true)
        {
            Thread.sleep(1000);
            logger.info("current value：" + index++);
        }
    }
}
```

log4j.properties

```properties
log4j.rootLogger=INFO,stdout,flume

log4j.appender.stdout = org.apache.log4j.ConsoleAppender
log4j.appender.stdout.target = System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} [%t] [%c] [%p] - %m%n
```



##11-4 -使用Flume采集Log4j产生的日志

**boy_streaming.conf**

```shell
#define name
agent1.sources=avro-source
agent1.channels=logger-channel
agent1.sinks=log-sink

#define source
agent1.sources.avro-source.type=avro
agent1.sources.avro-source.bind=0.0.0.0
agent1.sources.avro-source.port=41414

#define channel
agent1.channels.logger-channel.type=memory

#define sink
agent1.sinks.log-sink.type=logger

agent1.sources.avro-source.channels=logger-channel
agent1.sinks.log-sink.channel=logger-channel
```

启动flume

```shell
flume-ng agent \
--conf $FLUME_HOME/conf \
--conf-file $FLUME_HOME/conf/boy_streaming.conf \
--name agent1 \
-Dflume.root.logger=INFO,console
```

添加Log4J Appender到log4j.properties

```shell
log4j.rootLogger=INFO,stdout,flume

log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.target=System.out
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss,SSS} [%t] [%c] [%p] - %m%n


log4j.appender.flume=org.apache.flume.clients.log4jappender.Log4jAppender
log4j.appender.flume.Hostname=hadoop000
log4j.appender.flume.Port=41414
log4j.appender.flume.UnsafeMode=true
```

错误：

java.lang.ClassNotFoundException: org.apache.flume.clients.log4jappender.Log4jAppender

添加jar包

```shell
<dependency>
    <groupId>oarg.apache.flume.flume-ng-clients</groupId>
    <artifactId>flume-ng-log4jappender</artifactId>
    <version>1.6.0</version>
</dependency>
```



```scala
import org.apache.log4j.Logger;

/**
 * 模拟日志产生
 */

public class LoggerGenerator
{

    private static Logger logger = Logger.getLogger(LoggerGenerator.class.getName());

    public static void main(String[] args) throws InterruptedException
    {
        int index = 0;
        while (true)
        {
            Thread.sleep(1000);
            logger.info("current value：" + index++);
        }
    }
}
```



## 11-5 -使用KafkaSInk将Flume收集到的数据输出到Kafka

```shell
##启动Kafka
zkServer.sh start

##启动Kafka
kafka-server-start.sh -daemon /home/hadoop/app/kafka_2.11-0.9.0.0/config/server.properties

##查看创建topic
kafka-topics.sh --create --zookeeper hadoop000:2181 --replication-factor 1 --partitions 1 --topic boy_streamingtopic

kafka-topics.sh --list --zookeeper hadoop000:2181

##
```



**boy_streaming2.conf**

```shell
#define name
agent1.sources=avro-source
agent1.channels=logger-channel
agent1.sinks=kafka-sink

#define source
agent1.sources.avro-source.type=avro
agent1.sources.avro-source.bind=0.0.0.0
agent1.sources.avro-source.port=41414

#define channel
agent1.channels.logger-channel.type=memory

#define sink
agent1.sinks.kafka-sink.type=org.apache.flume.sink.kafka.KafkaSink
agent1.sinks.kafka-sink.topic = boy_streamingtopic
agent1.sinks.kafka-sink.brokerList = hadoop000:9092
agent1.sinks.kafka-sink.requiredAcks = 1
agent1.sinks.kafka-sink.batchSize = 20

agent1.sources.avro-source.channels=logger-channel
agent1.sinks.kafka-sink.channel=logger-channel
```

启动flume

```shell
flume-ng agent \
--conf $FLUME_HOME/conf \
--conf-file $FLUME_HOME/conf/boy_streaming2.conf \
--name agent1 \
-Dflume.root.logger=INFO,console
```

启动kafka消费

```shell
kafka-console-consumer.sh --zookeeper hadoop000:2181 --topic boy_streamingtopic
```

在Idea中进行测试 is ok



##11-6 -Spark Streaming消费Kafka的数据进行统计

```
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
```

添加参数：

```shell
hadoop000:2181 test boy_streamingtopic 1
```

运行is ok



## 11-7 -本地测试和生产环境使用的拓展

我们现在是在本地进行测试的，在IDEA中运行LoggerGenerator，
然后使用Flume、Kafka以及Spark Streaming进行处理操作。

在生产上肯定不是这么干的，怎么干呢？
1) 打包jar，执行LoggerGenerator类
2) Flume、Kafka和我们的测试是一样的
3) Spark Streaming的代码也是需要打成jar包，然后使用spark-submit的方式进行提交到环境上执行
	可以根据你们的实际情况选择运行模式：local/yarn/standalone/mesos

在生产上，整个流处理的流程都一样的，区别在于业务逻辑的复杂性





---

Boy-20180621