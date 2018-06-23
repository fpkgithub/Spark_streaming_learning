# 第9章 Spark Streaming整合Flume

本章节将讲解Spark Streaming整合Flume的两种方式，讲解如何在本地进行开发测试，如何在服务器上进行测试 

[TOC]

## 9-1 -课程目录

- 实战一：Flume-style Push-based Approach
- 实战二：Pull-based Approach using a Custom Sink



## 9-2 -Push方式整合之概述

**实战一：Flume-style Push-based Approach**

高级数据源 Advanced Sources：http://spark.apache.org/docs/latest/streaming-programming-guide.html#advanced-sources

[Flume Integration Guide](http://spark.apache.org/docs/latest/streaming-flume-integration.html) 



## 9-3 -Push方式整合之Flume Agent配置开发

**Push方式整合**

Flume Agent的编写： flume_push_streaming_boy.conf

```shell
simple-agent.sources = netcat-source
simple-agent.sinks = avro-sink
simple-agent.channels = memory-channel

simple-agent.sources.netcat-source.type = netcat
simple-agent.sources.netcat-source.bind = hadoop000
simple-agent.sources.netcat-source.port = 44444

simple-agent.sinks.avro-sink.type = avro
simple-agent.sinks.avro-sink.hostname = hadoop000
simple-agent.sinks.avro-sink.port = 41414

simple-agent.channels.memory-channel.type = memory

simple-agent.sources.netcat-source.channels = memory-channel
simple-agent.sinks.avro-sink.channel = memory-channel
```

虚拟机:   hadoop000:192.168.95.131 

## 9-4 -Push方式整合之Spark Streaming应用开发

hadoop000:是服务器的地址
local的模式进行Spark Streaming代码的测试  192.168.95.131

本地测试总结
1）启动sparkstreaming作业
2)  启动flume agent
3)  通过telnet输入数据，观察IDEA控制台的输出



## 9-5 -Push方式整合之本地环境联调

```
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Spark streaming 整合Flume的第一种方式
  */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    //TODO... 如何使用SparkStreaming整合Flume 
    val flumeStream = FlumeUtils.createStream(ssc, "0.0.0.0", 41414)  //0.0.0.0使用本地的

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
```

启动Flume

Flume Agent的编写： flume_push_streaming_boy.conf

```shell
simple-agent.sources = netcat-source
simple-agent.sinks = avro-sink
simple-agent.channels = memory-channel

##hadoop000服务器
simple-agent.sources.netcat-source.type = netcat
simple-agent.sources.netcat-source.bind = hadoop000
simple-agent.sources.netcat-source.port = 44444

##202.117.10.159  本地地址  sink到本地
simple-agent.sinks.avro-sink.type = avro
simple-agent.sinks.avro-sink.hostname = 202.117.10.159  
simple-agent.sinks.avro-sink.port = 41414

simple-agent.channels.memory-channel.type = memory

simple-agent.sources.netcat-source.channels = memory-channel
simple-agent.sinks.avro-sink.channel = memory-channel
```

本机电脑IP：202.117.10.159  

```shell
flume-ng agent  \
--name simple-agent   \
--conf $FLUME_HOME/conf    \
--conf-file $FLUME_HOME/conf/flume_push_streaming_boy.conf  \
-Dflume.root.logger=INFO,console
```

等于说在服务端s输入数据  通过hadoop000:44444，开启端口输入：

> Telnet协议是TCP/IP协议家族中的一员，是Internet远程登陆服务的标准协议和主要方式。它为用户提供了在本地计算机上完成远程主机工作的能力。 

```shell
##输入
$ telnet hadoop000 44444


##开启flume
flume-ng agent  \
--name simple-agent   \
--conf $FLUME_HOME/conf    \
--conf-file $FLUME_HOME/conf/flume_push_streaming_boy.conf  \
-Dflume.root.logger=INFO,console
```

设置输入参数：

```scala
if (args.length != 2) {
  System.err.print("Usage:FlumePushWordCount <hostname> <port>")
  System.exit(1)
}

val Array(hostname, port) = args
```

![image.png](https://upload-images.jianshu.io/upload_images/5959612-d09816b44ccab3d3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**本地测试总结**
1）启动sparkstreaming作业
2)  启动flume agent
3)  通过telnet输入数据，观察IDEA控制台的输出



## 9-6 -Push方式整合之服务器环境联调

打包

```shell
mvn package -DskipTests
```

修改flume_push_streaming_boy.conf ，sink到服务器端

```shell
simple-agent.sources = netcat-source
simple-agent.sinks = avro-sink
simple-agent.channels = memory-channel

##hadopp000服务器
simple-agent.sources.netcat-source.type = netcat
simple-agent.sources.netcat-source.bind = hadoop000
simple-agent.sources.netcat-source.port = 44444

##sink到服务器
simple-agent.sinks.avro-sink.type = avro
simple-agent.sinks.avro-sink.hostname = hadoop000
simple-agent.sinks.avro-sink.port = 41414

simple-agent.channels.memory-channel.type = memory

simple-agent.sources.netcat-source.channels = memory-channel
simple-agent.sinks.avro-sink.channel = memory-channel
```

提交

**注意点：先启动Spark Streaming应用程序，后启动flume**

```shell
##启动spark
spark-submit --class com.imooc.spark.FlumePushWordCount --master local[2] --packages org.apache.spark:spark-streaming-flume_2.11:2.2.0 /home/hadoop/lib/boy/sparktrain-1.0.jar hadoop000 41414

##启动flume
flume-ng agent  \
--name simple-agent   \
--conf $FLUME_HOME/conf    \
--conf-file $FLUME_HOME/conf/flume_push_streaming_boy.conf  \
-Dflume.root.logger=INFO,console


## 通过telnet输入数据
telnet localhost 44444
```



##9-7 -Pull方式整合之概述

实战二：Pull-based Approach using a Custom Sink

http://spark.apache.org/docs/latest/streaming-flume-integration.html#approach-2-pull-based-approach-using-a-custom-sink



**Pull方式整合**

Flume Agent的编写： flume_pull_streaming.conf

```shell
simple-agent.sources = netcat-source
simple-agent.sinks = spark-sink
simple-agent.channels = memory-channel

simple-agent.sources.netcat-source.type = netcat
simple-agent.sources.netcat-source.bind = hadoop000
simple-agent.sources.netcat-source.port = 44444

simple-agent.sinks.spark-sink.type = org.apache.spark.streaming.flume.sink.SparkSink
simple-agent.sinks.spark-sink.hostname = hadoop000
simple-agent.sinks.spark-sink.port = 41414

simple-agent.channels.memory-channel.type = memory

simple-agent.sources.netcat-source.channels = memory-channel
simple-agent.sinks.spark-sink.channel = memory-channel
```

添加依赖

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-streaming-flume-sink_2.11</artifactId>
    <version>${spark.version}</version>
</dependency>

<dependency>
    <groupId>org.apache.commons</groupId>
    <artifactId>commons-lang3</artifactId>
    <version>3.5</version>
</dependency>
```



```scala
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark streaming 整合Flume的第二种方式
  */
object FlumePullWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.print("Usage:FlumePullWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePullWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //TODO... 如何使用SparkStreaming整合Flume
    val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)

    flumeStream.map(x => new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
```



## 9-8 -Pull方式整合之Flume Agent配置开发



## 9-9 -Pull方式整合之Spark Streaming应用开发



## 9-10 -Pull方式整合之本地环境联调

**注意点：先启动flume 后启动Spark Streaming应用程序**

```shell
##启动flume
flume-ng agent  \
--name simple-agent   \
--conf $FLUME_HOME/conf    \
--conf-file $FLUME_HOME/conf/flume_pull_streaming_boy.conf  \
-Dflume.root.logger=INFO,console

flume-ng agent  --name simple-agent   --conf $FLUME_HOME/conf    --conf-file $FLUME_HOME/conf/flume_pull_streaming_boy.conf  -Dflume.root.logger=INFO,console

##启动telnet
telnet localhost 44444


##启动Spark Streaming应用程序
spark-submit \
--class com.imooc.spark.FlumePullWordCount \
--master local[2] \
--packages org.apache.spark:spark-streaming-flume_2.11:2.2.0 \
/home/hadoop/lib/boy/sparktrain-1.0.jar \
hadoop000 41414

spark-submit --class com.imooc.spark.FlumePullWordCount --master local[2] --packages org.apache.spark:spark-streaming-flume_2.11:2.2.0 /home/hadoop/lib/boy/sparktrain-1.0.jar hadoop000 41414
```



##9-11 -Pull方式整合之服务器环境联调

工作一般使用第二种，flume将数据采集过来，丢到sink中，然后Spark streaming到sink中去拿数据，这种方式更可靠



Approach 2: Pull-based Approach using a Custom Sink

Instead of Flume pushing data directly to Spark Streaming, this approach runs a custom Flume sink that allows the following.

- Flume pushes data into the sink, and the data stays buffered.
- Spark Streaming uses a [reliable Flume receiver](http://spark.apache.org/docs/latest/streaming-programming-guide.html#receiver-reliability) and transactions to pull data from the sink. Transactions succeed only after data is received and replicated by Spark Streaming.

This ensures stronger reliability and [fault-tolerance guarantees](http://spark.apache.org/docs/latest/streaming-programming-guide.html#fault-tolerance-semantics) than the previous approach. However, this requires configuring Flume to run a custom sink. Here are the configuration steps.





---

Boy-20180619