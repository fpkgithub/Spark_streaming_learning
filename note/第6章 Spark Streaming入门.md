# 第6章 Spark Streaming入门

本章节将讲解Spark Streaming是什么，了解Spark Streaming的应用场景及发展史，并从词频统计案例入手带大家了解Spark Streaming的工作原理

[TOC]



##6-1 -课程目录

![image.png](https://upload-images.jianshu.io/upload_images/5959612-40ad0009088cc0ab.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



## 6-2 -Spark Streaming概述

**Spark Streaming** is an extension of the core Spark API that enables scalable, 
high-throughput, 
fault-tolerant 
stream processing of live data streams.

官网：http://spark.apache.org/docs/latest/streaming-programming-guide.html

> Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. Data can be ingested from many sources like Kafka, Flume, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like `map`, `reduce`, `join` and `window`. Finally, processed data can be pushed out to filesystems, databases, and live dashboards. In fact, you can apply Spark’s [machine learning](http://spark.apache.org/docs/latest/ml-guide.html) and [graph processing](http://spark.apache.org/docs/latest/graphx-programming-guide.html) algorithms on data streams. 

![Spark Streaming](http://spark.apache.org/docs/latest/img/streaming-arch.png) 

**Spark Streaming个人的定义：**
	将不同的**数据源**的数据经过Spark Streaming**处理**之后将结果输出到**外部文件系统**

**特点**
	低延时
	能从错误中高效的恢复：fault-tolerant 
	能够运行在成百上千的节点
	能够将批处理、机器学习、图计算等子框架和Spark Streaming综合起来使用



**工作原理：**

>Internally, it works as follows. Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches. 

![Spark Streaming](http://spark.apache.org/docs/latest/img/streaming-flow.png) 

**Spark streaming 在spark中的位置**

![image.png](https://upload-images.jianshu.io/upload_images/5959612-0171f9539b1cc8a5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##6-3 -Spark Streaming应用场景

应用场景：

- 交易中实时欺诈检测
- 实时反应电子设备的检测
- 电商实时推荐
- 实时监控（公司的网络  项目的error错误收集）



##6-4 -Spark Streaming集成Spark生态系统的使用

![Spark生态系统](https://upload-images.jianshu.io/upload_images/5959612-db7d16ae2f6f2199.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

- 批处理和流处理的使用

![image.png](https://upload-images.jianshu.io/upload_images/5959612-1929bc065f90b629.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



- 离线的模型学习，在线应用

![1528778438423](F:\晓习\Hadoop\Imooc\Spark Streaming实时流处理项目实战\node\1528778438423.png)

- 使用SQL进行交互式的数据流查询

![使用SQL进行交互式的数据流查询](https://upload-images.jianshu.io/upload_images/5959612-eba49632c107e2b9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)





##6-5 -Spark Streaming发展史

![image.png](https://upload-images.jianshu.io/upload_images/5959612-1cdc0758ae8b75f9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##6-6 -从词频统计功能着手入门Spark Streaming

- spark-submit执行
- spark-shell执行

GitHub：https://github.com/apache/spark



开启端口：用于输入

```shell
nc -lk 9999
```



**spark-submit的使用**

使用spark-submit来提交我们的spark应用程序运行的脚本(**生产**)

```shell
./spark-submit --master local[2] \
--class org.apache.spark.examples.streaming.NetworkWordCount \
--name NetworkWordCount \
/home/hadoop/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/jars/spark-examples_2.11-2.2.0.jar hadoop000 9999

spark-submit --master local[2] --class org.apache.spark.examples.streaming.NetworkWordCount --name NetworkWordCount /home/hadoop/app/spark-2.2.0-bin-2.6.0-cdh5.7.0/examples/jars/spark-examples_2.11-2.2.0.jar hadoop000 9999
```



如何使用spark-shell来提交(**测试**)

```shell 
$ ./spark-shell --master local[2]

##
$ spark-shell --master local[2]

##提交作业
import org.apache.spark.streaming.{Seconds, StreamingContext}

val ssc = new StreamingContext(sc, Seconds(1))
val lines = ssc.socketTextStream("hadoop000", 9999)
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
wordCounts.print()
ssc.start()
ssc.awaitTermination()
```



##6-7 -Spark Streaming工作原理(粗粒度)

**工作原理：粗粒度**
Spark Streaming接收到实时数据流，把数据按照指定的时间段切成一片片小的数据块，
然后把小的数据块传给Spark Engine处理。

![image.png](https://upload-images.jianshu.io/upload_images/5959612-bae537c58319206a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##6-8 -Spark Streaming工作原理(细粒度)

![image.png](https://upload-images.jianshu.io/upload_images/5959612-3950416ed8ad9d1b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)









---

Boy-20180612

