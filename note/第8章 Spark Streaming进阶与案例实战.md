# 第8章 Spark Streaming进阶与案例实战

本章节将讲解Spark Streaming如何处理带状态的数据，通过案例让大家知道Spark Streaming如何写数据到MySQL，Spark Streaming如何整合Spark SQL进行操作 



[TOC]

##8-1 -课程目录

- 带状态的算子：UpdateStateByKey
- 实战：计算到目前为止累计出现的单词个数写入到Mysqlzhon
- 基于window的统计
- 实战：黑名单过滤
- 实战：Spark Streaming整合Spark SQL实战



##8-2 -实战之updateStateByKey算子的使用

带状态的算子：UpdateStateByKey

The `updateStateByKey` operation allows you to maintain arbitrary state while continuously updating it with new information. To use this, you will have to do two steps.

1. Define the state - The state can be an arbitrary data type.
2. Define the state update function - Specify with a function how to update the state using the previous state and the new values from an input stream.



**updateStateByKey算子**
需求：统计到目前为止**累计**出现的单词的个数(需要保持住以前的状态)

```
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成有状态统计
  * 累计求和功能
  */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 如果使用了stateful的算子，必须设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("hadoop000", 6789)
    val result = lines.flatMap(_.split(" ").map((_, 1)))
    val state = result.updateStateByKey[Int](updateFunction _)
    state.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 把当前的数据去更新已有的或者是老的数据
    *
    * @param currentValues
    * @param preValues
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }
}
```

开启6789端口

```shell
$ nc -lk 6789
```

错误

```shell
java.lang.IllegalArgumentException: requirement failed: The checkpoint directory has not been set. Please set it by StreamingContext.checkpoint().
```



##8-3 -实战之将统计结果写入到MySQL数据库中

实战：计算到目前为止累计出现的单词个数写入到MySQL

- 使用Spark Streaming进行统计分析
- Spark Streaming统计结果写入到MySQL



需求：将统计结果写入到MySQL

```sql
create table wordcount(
word varchar(50) default null,
wordcount int(10) default null
);
```

安装mysql8.0.11，修改密码，设置时区

```java
java.sql.SQLException: The server time zone value 'ÖÐ¹ú±ê×¼Ê±¼ä' is unrecognized or represents more than one time zone. You must configure either the server or JDBC driver (via the serverTimezone configuration property) to use a more specifc time zone value if you want to utilize time zone support.
```

```shell
set global time_zone='+8:00';
```



通过该sql将统计结果写入到MySQL
insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"

**存在的问题：**
1) 对于已有的数据做更新，而是所有的数据均为insert
	改进思路：
		a) 在插入数据前先判断单词是否存在，如果存在就update，不存在则insert
		b) 工作中：HBase/Redis

2) 每个rdd的partition创建connection，建议大家改成连接池



##8-4 -实战之窗口函数的使用

- 基于window的统计
-  http://spark.apache.org/docs/latest/streaming-programming-guide.html#window-operations

![基于window的统计](https://upload-images.jianshu.io/upload_images/5959612-3ae1580490248d00.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**window：**定时的进行一个时间段内的数据处理

- window length  ： 窗口的长度
- sliding interval： 窗口的间隔

这2个参数和我们的**batch size**有关系：倍数

每隔多久计算某个范围内的数据：每隔10秒计算前10分钟的wc

==> 每隔sliding interval统计前window length的值

![Spark Streaming滑动窗口](http://spark.apache.org/docs/latest/img/streaming-dstream-window.png) 

```
// Reduce last 30 seconds of data, every 10 seconds
val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(30), Seconds(10))
```



##8-5 -实战之黑名单过滤

**实战：黑名单过滤**

- transform算子的使用
- Spark Streaming 整合RDD进行操作

**需求：** 黑名单过滤

访问日志   ==> DStream
20180808,zs
20180808,ls
20180808,ww
   ==>  (zs: 20180808,zs)(ls: 20180808,ls)(ww: 20180808,ww)

黑名单列表  ==> RDD
zs
ls
   ==>(zs: true)(ls: true)



==> 20180808,ww

**leftjoin**
(zs: [<20180808,zs>, <true>])        x 
(ls: [<20180808,ls>, <true>])          x
(ww: [<20180808,ww>, <false>])  ==> tuple 1



```scala
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * transform：黑名单过滤
  *
  */
object TransformApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    /**
      * 构建黑名单
      */

    val blacks = List("zs", "ls")
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))

    val lines = ssc.socketTextStream("192.168.95.131", 6789)
    val clicklog = lines.map(x => (x.split(",")(1), x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => x._2._2.getOrElse(false) != true)
        .map(x => x._2._1)

      //getOrElse(false) != true   留下等于false的，等于true的过滤
    })

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
```



##8-6 -实战之Spark Streaming整合Spark SQL操作



```scala
package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
  * Spark Streaming 完成Spark SQL完成词频统计操作
  */
object SqlNetworkWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    val lines = ssc.socketTextStream("192.168.95.131", 6789)
    val words = lines.flatMap(_.split(" "))

    // Convert RDDs of the words DStream to DataFrame and run SQL query
    words.foreachRDD { (rdd: RDD[String], time: Time) =>
      // Get the singleton instance of SparkSession
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._

      // Convert RDD[String] to RDD[case class] to DataFrame
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      // Creates a temporary view using the DataFrame
      wordsDataFrame.createOrReplaceTempView("words")

      // Do word count on table using SQL and print it
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()

  }

  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)

  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

}
```





---

Boy-201806015







