# 第14章 Java拓展

本章节作为扩展内容，将带领大家使用Java来开始Spark应用程序，使得大家对于使用Scala以及Java来开发Spark应用程序都有很好的认识 

[TOC]

## 14-1 -课程目录

- 使用Java开发Spark Core应用程序
- 使用Java开发Spark Streaming应用程序



## 14-2 -使用Java开发Spark应用程序

```java
package com.imooc.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 使用Java开发Spark应用程序
 */
public class WordCountApp
{
    public static void main(String[] args)
    {
        SparkSession spark = SparkSession.builder().appName("WordCountApp").master("local[2]").getOrCreate();

        //TODO...处理我们的业务逻辑
        JavaRDD<String> lines = spark.read().textFile("data/test.log").javaRDD();

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Integer> counts = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<String, Integer> tuple : output)
        {
            System.out.println(tuple._1() + ":" + tuple._2());
        }

        spark.stop();
    }
}
```



## 14-3 -使用Java开发Spark Streaming应用程序

```java
package com.imooc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 使用Java开发Spark Streaming应用程序
 */
public class StreamingWordCoutApp
{
    public static void main(String[] args) throws InterruptedException
    {

        // Create a local StreamingContext with two working thread and batch interval of 1 second
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("StreamingWordCoutApp");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        //创建一个DStream(hostname + port)
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop000", 9999);

        JavaPairDStream<String, Integer> counts = lines.flatMap(line -> Arrays.asList(line.split("\t")).iterator()).
                mapToPair(word -> new Tuple2<String, Integer>(word, 1)).reduceByKey((x, y) -> x + y);

        //输出到控制台
        counts.print();

        jssc.start();
        jssc.awaitTermination();

    }
}
```

![捕获.PNG](https://upload-images.jianshu.io/upload_images/5959612-15b99fa8bc306458.PNG?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



---

Boy20180623



