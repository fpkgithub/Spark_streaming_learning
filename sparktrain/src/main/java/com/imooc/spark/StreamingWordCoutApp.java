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
