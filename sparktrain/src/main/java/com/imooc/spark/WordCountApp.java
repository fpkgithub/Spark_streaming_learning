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
