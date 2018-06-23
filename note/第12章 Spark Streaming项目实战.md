# 第12章 Spark Streaming项目实战

本章节将通过一个完整的项目实战让大家学会大数据实时流处理的各个环境的整合，如何根据业务需要来设计HBase的rowkey 

[TOC]

##12-1 -课程目录

将统计结果写入到数据库中

- 需求说明

- 互联网访问日志概述

- 功能开发及本地运行

- 生产环境运行

  

##12-2 -需求说明

- 今天到现在为止实战课程的访问量
- 今天到现在为止从搜索引擎引流过来的实战课程的访问量



##12-3 -用户行为日志介绍

为什么要记录用户访问行为日志？

- 网站页面的访问量
- 网站的粘性
- 推荐

用户行为日志内容

![用户行为日志内容](https://upload-images.jianshu.io/upload_images/5959612-6084af5d7636eb16.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

用户行为日志分析的意义

- 网站的眼睛
- 网站的神经
- 网站的大脑



##12-4 -Python日志产生器开发之产生访问url和ip信息

使用Python脚本实时产生数据

- Python实时日志产生器开发

generate_log.py

```pytho
#coding=UTF-8
import random

url_paths = [
	"class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821",
    "course/list",
]

ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

def sample_url():
    return random.sample(url_paths,1)[0]
	
def sample_ip():
	slice = random.sample(ip_slices, 4)
	return ".".join([str(item) for item in slice])
	
    
def generate_log(count=10):
	while count >= 1:
		query_log = "{url}\t{ip}".format(url=sample_url(),ip=sample_ip())
		print(query_log)
		count = count -1
		
if __name__ == '__main__':
	generate_log()
```



##12-5 -Python日志产生器开发之产生referer和状态码信息

```python
#coding=UTF-8
import random


url_paths = [
	"class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821",
    "course/list"
]

##生产ip
ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

##搜索来源
http_referers = [
	"https://www.baidu.com/s?wd={query}",
	"https://www.sogou.com/web?query={query}",
	"https://cn.bing.com/search?q={query}",
	"https://search.yahoo.com/search?p={query}"
]


search_keyword = [
	"Spark SQL实战",
	"Hadoop基础",
	"Storm实战",
	"Spark Streaming实战",
	"大数据面试"
]

status_codes = ["200","404","500"]

def sample_url():
    return random.sample(url_paths,1)[0]
	
def sample_ip():
	slice = random.sample(ip_slices, 4)
	return ".".join([str(item) for item in slice])
	
def sample_referer():
	if random.uniform(0,1) > 0.2:
		return "_"
	
	refer_str = random.sample(http_referers,1)
	query_str = random.sample(search_keyword,1)
	return refer_str[0].format(query=query_str[0])

def sample_status_codes():
	return random.sample(status_codes,1)[0]
    
def generate_log(count=10):
	while count >= 1:
		query_log = "{url}\t{ip}\t{referer}\t{status_codes}".format(url=sample_url(),ip=sample_ip(),referer=sample_referer(),status_codes=sample_status_codes())
		print(query_log)
		count = count -1
		
if __name__ == '__main__':
	generate_log()
```



##12-6 -Python日志产生器开发之产生日志访问时间

```python
#coding=UTF-8
import random
import time

url_paths = [
	"class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821",
    "course/list"
]

##生产ip
ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

##搜索来源
http_referers = [
	"https://www.baidu.com/s?wd={query}",
	"https://www.sogou.com/web?query={query}",
	"https://cn.bing.com/search?q={query}",
	"https://search.yahoo.com/search?p={query}"
]


search_keyword = [
	"Spark SQL实战",
	"Hadoop基础",
	"Storm实战",
	"Spark Streaming实战",
	"大数据面试"
]

status_codes = ["200","404","500"]

def sample_url():
    return random.sample(url_paths,1)[0]
	
def sample_ip():
	slice = random.sample(ip_slices, 4)
	return ".".join([str(item) for item in slice])
	
def sample_referer():
	if random.uniform(0,1) > 0.2:
		return "_"
	
	refer_str = random.sample(http_referers,1)
	query_str = random.sample(search_keyword,1)
	return refer_str[0].format(query=query_str[0])

def sample_status_codes():
	return random.sample(status_codes,1)[0]
    
def generate_log(count=10):
	time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	while count >= 1:
		query_log = "{local_time}\t{url}\t{ip}\t{referer}\t{status_codes}".format(local_time=time_str,url=sample_url(),ip=sample_ip(),referer=sample_referer(),status_codes=sample_status_codes())
		print(query_log)
		count = count -1
		
if __name__ == '__main__':
	generate_log()
```



## 12-7 -Python日志产生器服务器测试并将日志写入到文件中

```python
#coding=UTF-8
import random
import time

url_paths = [
	"class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821",
    "course/list"
]

##生产ip
ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

##搜索来源
http_referers = [
	"https://www.baidu.com/s?wd={query}",
	"https://www.sogou.com/web?query={query}",
	"https://cn.bing.com/search?q={query}",
	"https://search.yahoo.com/search?p={query}"
]


search_keyword = [
	"Spark SQL实战",
	"Hadoop基础",
	"Storm实战",
	"Spark Streaming实战",
	"大数据面试"
]

status_codes = ["200","404","500"]

def sample_url():
    return random.sample(url_paths,1)[0]
	
def sample_ip():
	slice = random.sample(ip_slices, 4)
	return ".".join([str(item) for item in slice])
	
def sample_referer():
	if random.uniform(0,1) > 0.2:
		return "_"
	
	refer_str = random.sample(http_referers,1)
	query_str = random.sample(search_keyword,1)
	return refer_str[0].format(query=query_str[0])

def sample_status_codes():
	return random.sample(status_codes,1)[0]
    
def generate_log(count=10):
	time_str = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	f = open("/home/hadoop/data/project/boy_logs/access.log","w+")
	while count >= 1:
		query_log = "{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{status_codes}\t{referer}".format(local_time=time_str,url=sample_url(),ip=sample_ip(),referer=sample_referer(),status_codes=sample_status_codes())
		print(query_log)
		f.write(query_log + "\n")
		count = count -1
		
if __name__ == '__main__':
	generate_log(100)
```



## 12-8 -通过定时调度工具每一分钟产生一批数据

设置自动执行脚本，产生日志数据

linux crontab
	网站：http://tool.lu/crontab
	每一分钟执行一次的crontab表达式： */1 * * * * 



```shell
$ vi boy_log_generator.sh
python /home/hadoop/data/project/boy_generate_log.py
$ chmod u+x boy_log_generator.sh

$ crontab -e
*/1 * * * * /home/hadoop/data/project/boy_log_generator.sh

##查看日志：
tail -200f  access.log
```



## 12-9 -使用Flume实时收集日志信息

对接python日志产生器输出的日志到Flume
**boy_streaming_project.conf**

选型：access.log  ==>  控制台输出
	exec
	memory
	logger（控制台）

```shell
exec-memory-logger.sources = exec-source
exec-memory-logger.sinks = logger-sink
exec-memory-logger.channels = memory-channel

exec-memory-logger.sources.exec-source.type = exec
exec-memory-logger.sources.exec-source.command = tail -F /home/hadoop/data/project/boy_logs/access.log
exec-memory-logger.sources.exec-source.shell = /bin/sh -c

exec-memory-logger.channels.memory-channel.type = memory

exec-memory-logger.sinks.logger-sink.type = logger

exec-memory-logger.sources.exec-source.channels = memory-channel
exec-memory-logger.sinks.logger-sink.channel = memory-channel 
```

启动flume

```shell
flume-ng agent \
--name exec-memory-logger \
--conf $FLUME_HOME/conf \
--conf-file /home/hadoop/data/project/boy_streaming_project.conf \
-Dflume.root.logger=INFO,console
```



##12-10 -对接实时日志数据到Kafka并输出到控制台测试

**日志=>Flume=>Kafka**

```shell
##启动zk：
zkServer.sh start

##启动Kafka Server
kafka-server-start.sh -daemon /home/hadoop/app/kafka_2.11-0.9.0.0/config/server.properties 
```

修改Flume配置文件使得flume sink数据到Kafka

**boy_streaming_project2.conf**

```shell
exec-memory-kafka.sources = exec-source
exec-memory-kafka.sinks = kafka-sink
exec-memory-kafka.channels = memory-channel

exec-memory-kafka.sources.exec-source.type = exec
exec-memory-kafka.sources.exec-source.command = tail -F /home/hadoop/data/project/boy_logs/access.log
exec-memory-kafka.sources.exec-source.shell = /bin/sh -c

exec-memory-kafka.channels.memory-channel.type = memory

exec-memory-kafka.sinks.kafka-sink.type = org.apache.flume.sink.kafka.KafkaSink
exec-memory-kafka.sinks.kafka-sink.brokerList = hadoop000:9092
exec-memory-kafka.sinks.kafka-sink.topic = boy_streamingtopic
exec-memory-kafka.sinks.kafka-sink.batchSize = 5
exec-memory-kafka.sinks.kafka-sink.requiredAcks = 1

exec-memory-kafka.sources.exec-source.channels = memory-channel
exec-memory-kafka.sinks.kafka-sink.channel = memory-channel
```

kafka来消费

```shell
kafka-console-consumer.sh --zookeeper hadoop000:2181 --topic boy_streamingtopic
```

启动flume

```shell
flume-ng agent \
--name exec-memory-kafka \
--conf $FLUME_HOME/conf \
--conf-file /home/hadoop/data/project/boy_streaming_project2.conf \
-Dflume.root.logger=INFO,console
```



## 12-11 -Spark Streaming对接Kafka的数据进行消费

- 在Spark应用程序接收到数据并完成记录数统计

```
package com.imooc.spark.project

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 测试步骤一：测试数据接收
    messages.map(_._2).count().print


    ssc.start()
    ssc.awaitTermination()

  }
}
```

参数

```shell
hadoop000:2181 test boy_streamingtopic 1
```



## 12-12 -使用Spark Streaming完成数据清洗操作

**数据清洗**

- 按照需求对实时产生的点击流数据进行数据清洗

数据清洗操作：从原始日志中取出我们所需要的字段信息就可以了

```
// 测试步骤二：数据清洗
val logs = messages.map(_._2)
val cleanData = logs.map(line => {

  val infos = line.split("\t")

  // infos(2) = "GET /class/130.html HTTP/1.1"
  // url = /class/130.html
  val url = infos(2).split(" ")(1)
  var courseId = 0

  // 把实战课程的课程编号拿到了
  if (url.startsWith("/class")) {
    val courseIdHTML = url.split("/")(2)
    courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
  }

  ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
}).filter(clicklog => clicklog.courseId != 0)

cleanData.print()
```

数据清洗结果类似如下：

```shell
ClickLog(46.30.10.167,20171022151701,128,200,-)
ClickLog(143.132.168.72,20171022151701,131,404,-)
ClickLog(10.55.168.87,20171022151701,131,500,-)
ClickLog(10.124.168.29,20171022151701,128,404,-)
ClickLog(98.30.87.143,20171022151701,131,404,-)
ClickLog(55.10.29.132,20171022151701,146,404,http://www.baidu.com/s?wd=Storm实战)
ClickLog(10.87.55.30,20171022151701,130,200,http://www.baidu.com/s?wd=Hadoop基础)
ClickLog(156.98.29.30,20171022151701,146,500,https://www.sogou.com/web?query=大数据面试)
ClickLog(10.72.87.124,20171022151801,146,500,-)
ClickLog(72.124.167.156,20171022151801,112,404,-)
```

到数据清洗完为止，日志中只包含了实战课程的日志



补充一点：希望你们的机器配置别太低
	Hadoop/ZK/HBase/Spark Streaming/Flume/Kafka
	hadoop000: 8Core  8G



## 12-13 -功能一之需求分析及存储结果技术选型分析

功能：统计今天到现在为止实战课程的访问量

**功能1：今天到现在为止 实战课程 的访问量**

	yyyyMMdd   courseid

**使用数据库来进行存储我们的统计结果**

>Spark Streaming把统计结果写入到数据库里面
>
>可视化前端根据：yyyyMMdd   courseid 把数据库里面的统计结果展示出来

**选择什么数据库作为统计结果的存储呢？**
**RDBMS: MySQL、Oracle...**
		day        course_id  click_count
		20171111     1            10
		20171111     2            10

​		下一个批次数据进来以后：
			20171111 + 1   ==> click_count + 下一个批次的统计结果  ==> 写入到数据库中

**NoSQL: HBase、Redis....**
		HBase： 一个API就能搞定，非常方便
			20171111 + 1 ==> click_count + 下一个批次的统计结果
		本次课程为什么要选择HBase的一个原因所在

**前提：**
	HDFS：start-dfs.sh
	Zookeeper：zkServer.sh start
	HBase：start-hbase.sh 





**HBase表设计**

```shell
##创建表
create 'boy_imooc_course_clickcount', 'info'


##查看
describe 'boy_imooc_course_clickcount'

##查看记录
scan 'boy_imooc_course_clickcount'

##Rowkey设计
day_courseid
```




##12-14 -功能一之数据库访问DAO层方法定义

如何使用Scala来操作HBase

```scala
//实战课程点击数实体类
case class CourseClickCount(day_course: String, click_count: Long)

//实战课程点击数-数据访问层
CourseClickCountDAO
save()
count()
```



##12-15 -功能一之HBase操作工具类开发

开发HBase操作工具类：Java工具类建议采用单例模式封装

```java
package com.imooc.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.view.Html;

import java.io.IOException;

/**
 * HBase操作工具类：Java工具类建议采用单例模式封装
 */
public class HBaseUtils
{
    HBaseAdmin admin = null;
    Configuration configuration = null;


    /**
     * 私有改造方法
     */
    private HBaseUtils()
    {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop000:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");

        try
        {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance()
    {

        if (null == instance)
        {
            instance = new HBaseUtils();
        }
        return instance;
    }


    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName)
    {
        HTable table = null;
        try
        {
            table = new HTable(configuration, tableName);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return table;
    }


    /**
     * 添加一条记录到HBase表
     *
     * @param tableName HBase表名
     * @param rowkey    HBase表的rowkey
     * @param cf        HBase表的columnfamily
     * @param column    HBase表的列
     * @param value     写入HBase表的值
     */
    public void put(String tableName, String rowkey, String cf, String column, String value)
    {
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try
        {
            table.put(put);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        //HTable table = HBaseUtils.getInstance().getTable("boy_imooc_course_clickcount");
        //System.out.println(table.getName().getNameAsString());

        String tableName = "boy_imooc_course_clickcount";
        String rowkey = "20171111_88";
        String cf = "info";
        String column = "click_count";
        String value = "2";

        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);

    }
}
```



## 12-16 -功能一之数据库访问DAO层方法实现

incrementColumnValue:根据已有ID和新值ID进行累加

```scala
package com.imooc.spark.project.dao

import com.imooc.spark.project.domain.CourseClickCount
import com.imooc.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


/**
  * 实战课程点击数-数据访问层
  */
object CourseClickCountDAO {

  val tableName = "boy_imooc_course_clickcount"
  //表名
  val cf = "info"
  //列簇
  val qualifer = "click_count" //列名

  /**
    * 保存数据到HBase
    *
    * @param list CourseClickCount集合
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    //incrementColumnValue:根据已有ID和新值ID进行累加
    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }


  /**
    * 根据rowkey查询值
    */
  def count(day_course: String): Long = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(day_course.getBytes())
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20171111_8", 8))
    list.append(CourseClickCount("20171111_7", 81))
    list.append(CourseClickCount("20171111_9", 9))
    list.append(CourseClickCount("20171111_1", 34))

    save(list)

    println(count("20171111_8") + " : " + count("20171111_7") + " : " + count("20171111_9") + " : " + count("20171111_1"))

  }

}
```



## 12-17 -功能一之将Spark Streaming的处理结果写入到HBase中

**测试步骤三：统计今天到现在为止实战课程的访问量**

```
package com.imooc.spark.project.spark

import com.imooc.spark.project.dao.CourseClickCountDAO
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount}
import com.imooc.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("Usage: ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //通过kafka接受到数据：Spark Streaming对接Kafka的数据进行消费
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 测试步骤一：测试数据接收
    //messages.map(_._2).count().print

    // 测试步骤二：数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {

      val infos = line.split("\t")

      // infos(2) = "GET /class/130.html HTTP/1.1"
      // url = /class/130.html
      val url = infos(2).split(" ")(1)
      var courseId = 0

      // 把实战课程的课程编号拿到了
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    //cleanData.print()

    // 测试步骤三：统计今天到现在为止实战课程的访问量
    cleanData.map(x => {
      //HBase rowkey设计： 20171111_88

      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {

          list.append(CourseClickCount(pair._1, pair._2))

        })
        CourseClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
```

启动flume后，结果如图所示

![1529586910536.png](https://upload-images.jianshu.io/upload_images/5959612-437eb98c7f367c03.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



##12-18 -功能二之需求分析及HBase设计&HBase数据访问层开发

功能：统计今天到现在为止从搜索引擎引流过来的实战课程的访问量

**功能二：功能一+从搜索引擎引流过来的**



HBase表设计

```shell
create 'boy_imooc_course_search_clickcount','info'

scan 'boy_imooc_course_search_clickcount'
```

rowkey设计：也是根据我们的业务需求来的

> 20171111 +search+ 1



创建domain:从搜索引擎过来的实战课程点击数实体类

```scala
case class CourseSearchClickCount(day_search_course: String, click_count: Long)
```

从搜索引擎过来的实战课程点击数-数据访问层

```scala
package com.imooc.spark.project.dao

import com.imooc.spark.project.domain.{CourseClickCount, CourseSearchClickCount}
import com.imooc.spark.project.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer


/**
  * 从搜索引擎过来的实战课程点击数-数据访问层
  */
object CourseSearchClickCountDAO {

  val tableName = "boy_imooc_course_search_clickcount"
  //表名
  val cf = "info"
  //列簇
  val qualifer = "click_count" //列名

  /**
    * 保存数据到HBase
    *
    * @param list CourseSearchClickCount集合
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    //incrementColumnValue:根据已有ID和新值ID进行累加
    for (ele <- list) {
      table.incrementColumnValue(Bytes.toBytes(ele.day_search_course),
        Bytes.toBytes(cf),
        Bytes.toBytes(qualifer),
        ele.click_count)
    }
  }


  /**
    * 根据rowkey查询值
    */
  def count(day_search_course: String): Long = {

    val table = HBaseUtils.getInstance().getTable(tableName)

    val get = new Get(day_search_course.getBytes())
    val value = table.get(get).getValue(cf.getBytes, qualifer.getBytes)

    if (value == null) {
      0L
    } else {
      Bytes.toLong(value)
    }
  }


  def main(args: Array[String]): Unit = {

    val list = new ListBuffer[CourseSearchClickCount]
    list.append(CourseSearchClickCount("20171111_8_baidu.com", 8))
    list.append(CourseSearchClickCount("20171111_9_cn.bing.com", 9))

    save(list)

    println(count("20171111_8_baidu.com") + " : " + count("20171111_9_cn.bing.com"))

  }

}
```



## 12-19 -功能二之功能实现及本地测试

Hbase清除表中数据

truncate 'boy_imooc_course_search_clickcount'





**测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量**

```
package com.imooc.spark.project.spark

import com.imooc.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.imooc.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object ImoocStatStreamingApp {

  def main(args: Array[String]): Unit = {

	//hadoop000:2181 test boy_streamingtopic 1
    if (args.length != 4) {
      println("Usage: ImoocStatStreamingApp <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val sparkConf = new SparkConf().setAppName("ImoocStatStreamingApp").setMaster("local[5]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //通过kafka接受到数据：Spark Streaming对接Kafka的数据进行消费
    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    // 测试步骤一：测试数据接收
    //messages.map(_._2).count().print

    // 测试步骤二：数据清洗
    val logs = messages.map(_._2)
    val cleanData = logs.map(line => {

      val infos = line.split("\t")

      // infos(2) = "GET /class/130.html HTTP/1.1"
      // url = /class/130.html
      val url = infos(2).split(" ")(1)
      var courseId = 0

      // 把实战课程的课程编号拿到了
      if (url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }

      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog => clicklog.courseId != 0)

    //cleanData.print()

    // 测试步骤三：统计今天到现在为止实战课程的访问量
    cleanData.map(x => {
      //HBase rowkey设计： 20171111_88

      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseClickCount]

        partitionRecords.foreach(pair => {

          list.append(CourseClickCount(pair._1, pair._2))

        })
        CourseClickCountDAO.save(list)
      })
    })


    // 测试步骤四：统计从搜索引擎过来的今天到现在为止实战课程的访问量

    cleanData.map(x => {

      /**
        * https://www.sogou.com/web?query=Spark SQL实战
        *
        * ==>
        *
        * https:/www.sogou.com/web?query=Spark SQL实战
        */
      val referer = x.referer.replaceAll("//", "/")
      val splits = referer.split("/")
      var host = ""
      if (splits.length > 2) {
        host = splits(1)
      }

      (host, x.courseId, x.time)
    }).filter(_._1 != null).map(x => {
      ((x._3.substring(0, 8) + "_" + x._1 + "_" + x._2), 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecords => {

        val list = new ListBuffer[CourseSearchClickCount]

        partitionRecords.foreach(pair => {

          list.append(CourseSearchClickCount(pair._1, pair._2))

        })
        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
```



```shell
hbase(main):016:0> scan 'boy_imooc_course_search_clickcount'
ROW                                    COLUMN+CELL                                                                                                    
 20180621__112                         column=info:click_count, timestamp=1529589118668, value=\x00\x00\x00\x00\x00\x00\x00h                          
 20180621__128                         column=info:click_count, timestamp=1529589118741, value=\x00\x00\x00\x00\x00\x00\x00U                          
 20180621__130                         column=info:click_count, timestamp=1529589118706, value=\x00\x00\x00\x00\x00\x00\x00X                          
 20180621__131                         column=info:click_count, timestamp=1529589118667, value=\x00\x00\x00\x00\x00\x00\x00^                          
 20180621__145                         column=info:click_count, timestamp=1529589118744, value=\x00\x00\x00\x00\x00\x00\x00^                          
 20180621__146                         column=info:click_count, timestamp=1529589118706, value=\x00\x00\x00\x00\x00\x00\x00U                          
 20180621_cn.bing.com_112              column=info:click_count, timestamp=1529589118692, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_cn.bing.com_128              column=info:click_count, timestamp=1529588939912, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_cn.bing.com_130              column=info:click_count, timestamp=1529589118721, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_cn.bing.com_131              column=info:click_count, timestamp=1529588939911, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_cn.bing.com_145              column=info:click_count, timestamp=1529589118649, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_cn.bing.com_146              column=info:click_count, timestamp=1529588939981, value=\x00\x00\x00\x00\x00\x00\x00\x02                       
 20180621_search.yahoo.com_112         column=info:click_count, timestamp=1529588939974, value=\x00\x00\x00\x00\x00\x00\x00\x0A                       
 20180621_search.yahoo.com_128         column=info:click_count, timestamp=1529589118697, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_search.yahoo.com_130         column=info:click_count, timestamp=1529588998610, value=\x00\x00\x00\x00\x00\x00\x00\x02                       
 20180621_search.yahoo.com_131         column=info:click_count, timestamp=1529589118697, value=\x00\x00\x00\x00\x00\x00\x00\x07                       
 20180621_search.yahoo.com_145         column=info:click_count, timestamp=1529589118779, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_search.yahoo.com_146         column=info:click_count, timestamp=1529589058788, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_112            column=info:click_count, timestamp=1529588939965, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_www.baidu.com_128            column=info:click_count, timestamp=1529589118792, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_130            column=info:click_count, timestamp=1529589058756, value=\x00\x00\x00\x00\x00\x00\x00\x03                       
 20180621_www.baidu.com_131            column=info:click_count, timestamp=1529589118795, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_145            column=info:click_count, timestamp=1529589058717, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_146            column=info:click_count, timestamp=1529589058713, value=\x00\x00\x00\x00\x00\x00\x00\x05                       
 20180621_www.sogou.com_112            column=info:click_count, timestamp=1529588998626, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.sogou.com_128            column=info:click_count, timestamp=1529588998610, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_www.sogou.com_130            column=info:click_count, timestamp=1529589118668, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_www.sogou.com_131            column=info:click_count, timestamp=1529588998631, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_www.sogou.com_145            column=info:click_count, timestamp=1529588998617, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_www.sogou.com_146            column=info:click_count, timestamp=1529588939937, value=\x00\x00\x00\x00\x00\x00\x00\x02                       
30 row(s) in 0.0640 seconds

hbase(main):017:0> scan 'boy_imooc_course_search_clickcount'
ROW                                    COLUMN+CELL                                                                                                    
 20180621__112                         column=info:click_count, timestamp=1529589178614, value=\x00\x00\x00\x00\x00\x00\x00s                          
 20180621__128                         column=info:click_count, timestamp=1529589178646, value=\x00\x00\x00\x00\x00\x00\x00c                          
 20180621__130                         column=info:click_count, timestamp=1529589178637, value=\x00\x00\x00\x00\x00\x00\x00e                          
 20180621__131                         column=info:click_count, timestamp=1529589178621, value=\x00\x00\x00\x00\x00\x00\x00e                          
 20180621__145                         column=info:click_count, timestamp=1529589178655, value=\x00\x00\x00\x00\x00\x00\x00g                          
 20180621__146                         column=info:click_count, timestamp=1529589178619, value=\x00\x00\x00\x00\x00\x00\x00\x5C                       
 20180621_cn.bing.com_112              column=info:click_count, timestamp=1529589178637, value=\x00\x00\x00\x00\x00\x00\x00\x09                       
 20180621_cn.bing.com_128              column=info:click_count, timestamp=1529588939912, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_cn.bing.com_130              column=info:click_count, timestamp=1529589118721, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_cn.bing.com_131              column=info:click_count, timestamp=1529588939911, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_cn.bing.com_145              column=info:click_count, timestamp=1529589118649, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_cn.bing.com_146              column=info:click_count, timestamp=1529588939981, value=\x00\x00\x00\x00\x00\x00\x00\x02                       
 20180621_search.yahoo.com_112         column=info:click_count, timestamp=1529588939974, value=\x00\x00\x00\x00\x00\x00\x00\x0A                       
 20180621_search.yahoo.com_128         column=info:click_count, timestamp=1529589178644, value=\x00\x00\x00\x00\x00\x00\x00\x05                       
 20180621_search.yahoo.com_130         column=info:click_count, timestamp=1529589178644, value=\x00\x00\x00\x00\x00\x00\x00\x03                       
 20180621_search.yahoo.com_131         column=info:click_count, timestamp=1529589118697, value=\x00\x00\x00\x00\x00\x00\x00\x07                       
 20180621_search.yahoo.com_145         column=info:click_count, timestamp=1529589178647, value=\x00\x00\x00\x00\x00\x00\x00\x09                       
 20180621_search.yahoo.com_146         column=info:click_count, timestamp=1529589058788, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_112            column=info:click_count, timestamp=1529588939965, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_www.baidu.com_128            column=info:click_count, timestamp=1529589178658, value=\x00\x00\x00\x00\x00\x00\x00\x05                       
 20180621_www.baidu.com_130            column=info:click_count, timestamp=1529589178625, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_131            column=info:click_count, timestamp=1529589178652, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_www.baidu.com_145            column=info:click_count, timestamp=1529589058717, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
 20180621_www.baidu.com_146            column=info:click_count, timestamp=1529589058713, value=\x00\x00\x00\x00\x00\x00\x00\x05                       
 20180621_www.sogou.com_112            column=info:click_count, timestamp=1529589178650, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_www.sogou.com_128            column=info:click_count, timestamp=1529588998610, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_www.sogou.com_130            column=info:click_count, timestamp=1529589118668, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_www.sogou.com_131            column=info:click_count, timestamp=1529588998631, value=\x00\x00\x00\x00\x00\x00\x00\x06                       
 20180621_www.sogou.com_145            column=info:click_count, timestamp=1529588998617, value=\x00\x00\x00\x00\x00\x00\x00\x08                       
 20180621_www.sogou.com_146            column=info:click_count, timestamp=1529589178649, value=\x00\x00\x00\x00\x00\x00\x00\x04                       
30 row(s) in 0.0420 seconds

```



## 12-20 -将项目运行在服务器环境中

- 编译打包
- 运行



项目打包：mvn clean package -DskipTests

报错：
[ERROR] /Users/rocky/source/work/sparktrain/src/main/scala/com/imooc/spark/project/dao/CourseClickCountDAO.scala:4: error: object HBaseUtils is not a member 
of package com.imooc.spark.project.utils



注解

```
<sourceDirectory>src/main/scala</sourceDirectory>
<testSourceDirectory>src/test/scala</testSourceDirectory>
```



**前提**

HDFS

Hbase

Zookeeper

Kafka： kafka-server-start.sh -daemon /home/hadoop/app/kafka_2.11-0.9.0.0/config/server.properties

Flume

```shell
flume-ng agent \
--name exec-memory-kafka \
--conf $FLUME_HOME/conf \
--conf-file /home/hadoop/data/project/boy_streaming_project2.conf \
-Dflume.root.logger=INFO,console
```



日志和jar包



**提交jar**

```shell
spark-submit --master local[5] \
--class com.imooc.spark.project.spark.ImoocStatStreamingApp \
/home/hadoop/lib/sparktrain-1.0.jar \
hadoop000:2181 test boy_streamingtopic 1
```

**报错：**

```shell
Exception in thread "main" java.lang.NoClassDefFoundError: org/apache/spark/streaming/kafka/KafkaUtils$
	at com.imooc.spark.project.spark.ImoocStatStreamingApp$.main(ImoocStatStreamingApp.scala:31)
	at com.imooc.spark.project.spark.ImoocStatStreamingApp.main(ImoocStatStreamingApp.scala)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.spark.deploy.SparkSubmit$.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:755)
	at org.apache.spark.deploy.SparkSubmit$.doRunMain$1(SparkSubmit.scala:180)
	at org.apache.spark.deploy.SparkSubmit$.submit(SparkSubmit.scala:205)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:119)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: java.lang.ClassNotFoundException: org.apache.spark.streaming.kafka.KafkaUtils$
	at java.net.URLClassLoader.findClass(URLClassLoader.java:381)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
	at java.lang.ClassLoader.loadClass(ClassLoader.java:357)
	... 11 more
```

添加packages

```shell
spark-submit --master local[5] \
--class com.imooc.spark.project.spark.ImoocStatStreamingApp \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
/home/hadoop/lib/sparktrain-1.0.jar \
hadoop000:2181 test boy_streamingtopic 1
```



报错:HBase的依赖包没有打到Jar里面

```shell
java.lang.NoClassDefFoundError: org/apache/hadoop/hbase/client/HBaseAdmin
	at com.imooc.spark.project.utils.HBaseUtils.<init>(HBaseUtils.java:30)
	at com.imooc.spark.project.utils.HBaseUtils.getInstance(HBaseUtils.java:40)
	at com.imooc.spark.project.dao.CourseClickCountDAO$.save(CourseClickCountDAO.scala:26)
	at com.imooc.spark.project.spark.ImoocStatStreamingApp$$anonfun$main$4$$anonfun$apply$1.a
```

添加hbase jars包

```shell
spark-submit --master local[5] \
--jars $(echo /home/hadoop/app/hbase-1.2.0-cdh5.7.0/lib/*.jar | tr ' ' ',') \
--class com.imooc.spark.project.spark.ImoocStatStreamingApp \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
/home/hadoop/lib/sparktrain-1.0.jar \
hadoop000:2181 test boy_streamingtopic 1
```

然后Hbase shell查看

```shell
scan 'boy_imooc_course_clickcount'

scan 'boy_imooc_course_search_clickcount'
```



提交作业时，注意事项：
1）--packages的使用
2）--jars的使用





---

Boy-20180622



