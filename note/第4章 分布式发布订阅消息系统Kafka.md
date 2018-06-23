

# 第4章 分布式发布订阅消息系统Kafka 

本章节将讲解Kafka的架构以及核心概念，Kafka环境的部署及脚本的使用，Kafka API编程，并通过Kafka容错性测试让大家体会到Kakfa的高可用性，并将Flume和Kafka整合起来开发一个功能

[TOC]



## 4-1 -课程目录

- Kafka概述
- Kafla架构及核心概念
- Kafka部署及使用
- Kafka容错性测试
- Kafka API编程
- Kafka实战



##4-2 -Kafka概述

- kafka.apache.org

Kafka概述：消息系统，消息中间件，和消息系统类似

妈妈：生产者
	你：消费者
	馒头：数据流、消息

```tex
正常情况下： 生产一个  消费一个
其他情况：  
        一直生产，你吃到某一个馒头时，你卡主(机器故障)， 馒头就丢失了
        一直生产，做馒头速度快，你吃来不及，馒头也就丢失了

拿个碗/篮子，馒头做好以后先放到篮子里，你要吃的时候去篮子里面取出来吃

篮子/框： Kafka
		当篮子满了，馒头就装不下了，咋办？ 
		多准备几个篮子 === Kafka的扩容
```


##4-3 -Kafka架构及核心概念

Kafka架构

> producer：生产者，就是生产馒头(老妈)
>
> consumer：消费者，就是吃馒头的(你)
>
> broker：篮子
>
> topic：主题，给馒头带一个标签，topica的馒头是给你吃的，topicb的馒头是给你弟弟吃

![Kafka架构](https://upload-images.jianshu.io/upload_images/5959612-41b9771bae033f1e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##4-4 -Kafka单节点单Broker部署之Zookeeper安装

- 单节点单Broker部署及使用

- 单节点多Broker部署及使用

- 多节点多Broker部署及使用

   

按照zookeeper：zookeeper-3.4.5-cdh5.7.0

解压：/home/hadoop/app

配置环境变量：

```shell
export ZK_HOME=/home/hadoop/app/zookeeper-3.4.5-cdh5.7.0
export PATH=$ZK_HOME/bin:$PATH
```

配置/home/hadoop/app/zookeeper-3.4.5-cdh5.7.0/conf/zoo.cfg

```shell
dataDir=/home/hadoop/app/tmp/zk
```

启动zookeeper

```shell
$ zkServer.sh start
$ jps
QuorumPeerMain
```



##4-5 -Kafka单节点单broker的部署及使用

下载地址：http://kafka.apache.org/downloads

kafka版本：Scala 2.11  - [kafka_2.11-0.9.0.0.tgz](https://archive.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz) ([asc](https://archive.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz.asc), [md5](https://archive.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz.md5))



解压

环境配置

配置文件

**server.properties**

```shell
broker.id=0   broker ID
listeners  监听端口
host.name  主机名
log.dirs   日志路径
zookeeper.connect    zookeeper地址
```



**启动Kafka**

```shell
$ kafka-server-start.sh

USAGE: /home/hadoop/app/kafka_2.11-0.9.0.0/bin/kafka-server-start.sh [-daemon] server.properties [--override property=value]*

$ kafka-server-start.sh $KAFKA_HOME/config/server.properties

##停止
$ kafka-server-stop.sh $KAFKA_HOME/config/server.properties
```

**创建topic: **指定zk

```shell
kafka-topics.sh --create --zookeeper hadoop000:2181 --replication-factor 1 --partitions 1 --topic hello_topic
```

**查看所有topic**

```shell
kafka-topics.sh --list --zookeeper hadoop000:2181
```

**发送消息: broker**：指定broker

```shell
kafka-console-producer.sh --broker-list hadoop000:9092 --topic hello_topic
```

**消费消息:** 指定zk

```shell
kafka-console-consumer.sh --zookeeper hadoop000:2181 --topic hello_topic --from-beginning
```

--from-beginning的使用

查看所有topic的详细信息：

```shell
kafka-topics.sh --describe --zookeeper hadoop000:2181
```

查看指定topic的详细信息：

```shell
kafka-topics.sh --describe --zookeeper hadoop000:2181 --topic hello_topic
```



##4-6 -Kafka单节点多broker部署及使用

单节点多broker

```shell
server-1.properties
	log.dirs=/home/hadoop/app/tmp/kafka-logs-1
	listeners=PLAINTEXT://:9093
	broker.id=1

server-2.properties
	log.dirs=/home/hadoop/app/tmp/kafka-logs-2
	listeners=PLAINTEXT://:9094
	broker.id=2

server-3.properties
	log.dirs=/home/hadoop/app/tmp/kafka-logs-3
	listeners=PLAINTEXT://:9095
	broker.id=3
```

启动Kafka1、2、3

```shell
kafka-server-start.sh -daemon $KAFKA_HOME/config/server-1.properties &
kafka-server-start.sh -daemon $KAFKA_HOME/config/server-2.properties &
kafka-server-start.sh -daemon $KAFKA_HOME/config/server-3.properties &
```



```shell
##创建topic
kafka-topics.sh --create --zookeeper hadoop000:2181 --replication-factor 3 --partitions 1 --topic boy-replicated-topic

##消费者
kafka-console-producer.sh --broker-list hadoop000:9093,hadoop000:9094,hadoop000:9095 --topic boy-replicated-topic

##生产者
kafka-console-consumer.sh --zookeeper hadoop000:2181 --topic boy-replicated-topic

##描述
kafka-topics.sh --describe --zookeeper hadoop000:2181 --topic boy-replicated-topic
```



多节点多Broker部署及使用：略

##4-7 -Kafka容错性测试

kill命令：

```shell
$ kill -9 PID
```



##4-8 -使用IDEA+Maven构建开发环境

- IDEA+Maven构建开发环境
- Producer API 的使用
- Consumer API的使用

![image.png](https://upload-images.jianshu.io/upload_images/5959612-6e7bf180c0c4d96c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##4-9 -Kafka Producer Java API编程



```java
kafka常用配置文件：KafkaProperties
生产者：KafkaProducer
Kafka Java API测试：KafkaClientApp
```

Win10远程连接虚拟机出错：

```java
Exception in thread "Thread-0" kafka.common.FailedToSendMessageException: Failed to send messages after 3 tries.
	at kafka.producer.async.DefaultEventHandler.handle(DefaultEventHandler.scala:91)
	at kafka.producer.Producer.send(Producer.scala:77)
	at kafka.javaapi.producer.Producer.send(Producer.scala:33)
	at com.imooc.spark.kafka.KafkaProducer.run(KafkaProducer.java:37)
```

在C:\Windows\System32\drivers\etc\hosts中添加对应的IP映射



##4-10 -Kafka Consumer Java API编程



```java
//Kafka Java API测试
new KafkaConsumer(KafkaProperties.TOPIC).start();

//Kafka消费者：指定zk
KafkaConsumer extends Thread
```



##4-11 -Kafka实战之整合Flume和Kafka完成实时数据采集

- 整合Flume和Kafka完成实时数据采集

**web  -->  [ exec source   ->   memory channel  ->  avro sink ]   -->  [  avro sink  ->  memory channel  -> kafka consumer]**

![数据采集](https://upload-images.jianshu.io/upload_images/5959612-626a52ff84fc644f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



**整合Flume和Kafka的综合使用**

**avro-memory-kafka.conf**

```shell
avro-memory-kafka.sources = avro-source
avro-memory-kafka.sinks = kafka-sink
avro-memory-kafka.channels = memory-channel

avro-memory-kafka.sources.avro-source.type = avro
avro-memory-kafka.sources.avro-source.bind = hadoop000
avro-memory-kafka.sources.avro-source.port = 44444

avro-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
avro-memory-kafka.sinks.kafka-sink.brokerList = hadoop000:9092
avro-memory-kafka.sinks.kafka-sink.topic = boy_topic
avro-memory-kafka.sinks.kafka-sink.batchSize = 5
avro-memory-kafka.sinks.kafka-sink.requiredAcks =1 

avro-memory-kafka.channels.memory-channel.type = memory

avro-memory-kafka.sources.avro-source.channels = memory-channel
avro-memory-kafka.sinks.kafka-sink.channel = memory-channel
```



**启动flume**

```shell
flume-ng agent \
--name avro-memory-kafka  \
--conf $FLUME_HOME/conf  \
--conf-file $FLUME_HOME/conf/avro-memory-kafka.conf \
-Dflume.root.logger=INFO,console

flume-ng agent \
--name exec-memory-avro  \
--conf $FLUME_HOME/conf  \
--conf-file $FLUME_HOME/conf/exec-memory-avro.conf \
-Dflume.root.logger=INFO,console
```

**启动kafka**

```shell
kafka-console-consumer.sh --zookeeper hadoop000:2181 --topic boy_topic
```





---

**Boy-20180611**





