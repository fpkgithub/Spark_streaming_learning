# 第7章 Spark Streaming核心概念与编程

本章节将讲解Spark Streaming中的核心概念、常用操作，通过Spark Streaming如何操作socket以及HDFS上的数据让大家进一步了解Spark Streaming的编程 



[TOC]

##7-1 -课程目录

- 核心概念
- Transformations
- Output Operations
- 实战案例



##7-2 -核心概念之StreamingContext

**核心概念：**
StreamingContext ：http://spark.apache.org/docs/latest/streaming-programming-guide.html#initializing-streamingcontext

```scala
//构造函数 
def this(sparkContext: SparkContext, batchDuration: Duration) = {
    this(sparkContext, null, batchDuration)
}

def this(conf: SparkConf, batchDuration: Duration) = {
    this(StreamingContext.createNewSparkContext(conf), null, batchDuration)
}
```

batch interval可以根据你的应用程序需求的延迟要求以及集群可用的资源情况来设置



一旦StreamingContext定义好之后，就可以做一些事情

>1. Define the input sources by creating input DStreams.
>2. Define the streaming computations by applying transformation and output operations to DStreams.
>3. Start receiving data and processing it using `streamingContext.start()`.
>4. Wait for the processing to be stopped (manually or due to any error) using `streamingContext.awaitTermination()`.
>5. The processing can be manually stopped using `streamingContext.stop()`.



要记住的要点 :

>- Once a context has been started, no new streaming computations can be set up or added to it.
>- Once a context has been stopped, it cannot be restarted.
>- Only one StreamingContext can be active in a JVM at the same time.
>- stop() on StreamingContext also stops the SparkContext. To stop only the StreamingContext, set the optional parameter of `stop()` called `stopSparkContext` to false.
>- A SparkContext can be re-used to create multiple StreamingContexts, as long as the previous StreamingContext is stopped (without stopping the SparkContext) before the next StreamingContext is created.





##7-3 -核心概念之DStream

DStream：http://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams

Discretized Streams (DStreams)

>Internally, a DStream is represented by a continuous series of RDDs（实际上，DStream代表一系列持续的RDD）
>Each RDD in a DStream contains data from a certain interval（每一个在DStream中的RDD都代表着某个批次）

![Spark Streaming](http://spark.apache.org/docs/latest/img/streaming-dstream.png) 

Any operation applied on a DStream translates to operations on the underlying RDDs.  

> 一个DStream由多个RDD构成，对于DStream的操作底层都是基于RDD；
>
> 对DStream操作算子，比如map/flatMap，其实底层会被翻译为对DStream中的每个RDD都做相同的操作；
> 因为一个DStream是由不同批次的RDD所构成的。

![Spark Streaming](http://spark.apache.org/docs/latest/img/streaming-dstream-ops.png) 



##7-4 -核心概念之Input DStreams和Receivers

Input Dstreams and Receives : http://spark.apache.org/docs/latest/streaming-programming-guide.html#input-dstreams-and-receivers

Every input DStream (except file stream, discussed later in this section) 
is associated with a Receiver object which 
receives the data from a source and stores it 
in Spark’s memory for processing.

每个输入DStream（文件流除外）都与Receiver对象相关联，该对象从源接收数据并将其存储在Spark的内存中进行处理。 



##7-5 -核心概念之Transformation和Output Operations

官网：http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams

与RDD类似，转换允许修改输入DStream中的数据。 DStreams支持Spark Spark RDD的许多转换。一些常见的如下。 

![Transformations on DStreams](https://upload-images.jianshu.io/upload_images/5959612-b04d201fddb4c3c6.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Output Operations on DStreams：http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams

![Output Operations on DStreams](https://upload-images.jianshu.io/upload_images/5959612-127f3dece2058f5d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##7-6 -案例实战之Spark Streaming处理socket数据

- Spark Streaming处理socket数据

```scala
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

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
```

Shell开启端口监听：

```shell
nc -lk 6789
```



错误1：

```scala
<dependency>
    <groupId>com.fasterxml.jackson.module</groupId>
    <artifactId>jackson-module-scala_2.11</artifactId>
    <version>2.6.5</version>
</dependency>
```

错误2：

```shell
java.lang.NoClassDefFoundError: net/jpountz/util/SafeUtils

//添加jpountz依赖
<dependency>
    <groupId>net.jpountz.lz4</groupId>
    <artifactId>lz4</artifactId>
    <version>1.3.0</version>
</dependency>

```



![local[1]](https://upload-images.jianshu.io/upload_images/5959612-cdb68e227c723176.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##7-7 -案例实战之Spark Streaming处理文件系统数据

- Spark Streaming处理HDFS文件数据

FileWordCount.scala

```
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理HDFS文件数据
  */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("file:///D:/IDE/Idea/sparktrain/data/imooc")
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
```



注意：本地文件只能moving到监控的文件夹下面，对于监控文件夹里面的文件修改都是无用的

```
Files must be written to the monitored directory by "moving" them from another location within the same file system. 
```



[How Directories are Monitored](http://spark.apache.org/docs/latest/streaming-programming-guide.html#how-directories-are-monitored)

Spark Streaming will monitor the directory `dataDirectory` and process any files created in that directory.

- A simple directory can be monitored, such as `"hdfs://namenode:8040/logs/"`. All files directly under such a path will be processed as they are discovered.
- A [POSIX glob pattern](http://pubs.opengroup.org/onlinepubs/009695399/utilities/xcu_chap02.html#tag_02_13_02) can be supplied, such as `"hdfs://namenode:8040/logs/2017/*"`. Here, the DStream will consist of all files in the directories matching the pattern. That is: it is a pattern of directories, not of files in directories.
- All files must be in the same data format.
- A file is considered part of a time period based on its modification time, not its creation time.
- Once processed, changes to a file within the current window will not cause the file to be reread. That is: *updates are ignored*.
- The more files under a directory, the longer it will take to scan for changes — even if no files have been modified.
- If a wildcard is used to identify directories, such as `"hdfs://namenode:8040/logs/2016-*"`, renaming an entire directory to match the path will add the directory to the list of monitored directories. Only the files in the directory whose modification time is within the current window will be included in the stream.
- Calling [`FileSystem.setTimes()`](https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html#setTimes-org.apache.hadoop.fs.Path-long-long-) to fix the timestamp is a way to have the file picked up in a later window, even if its contents have not changed.





---

Boy-20180613