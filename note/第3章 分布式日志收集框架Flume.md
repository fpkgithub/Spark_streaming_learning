# 第3章 分布式日志收集框架Flume

本章节将从通过一个业务场景出发引出Flume的产生背景，将讲解Flume的架构及核心组件，Flume环境部署以及Flume Agent开发实战让大家学会如何使用Flume来进行日志的采集 

[TOC]



## 3-1 -课程目录

1、业务现状分析  

2、Flume概述  

3、Flume架构及核心组件  

4、Flume环境部署

5、Flume实战



##3-2 -业务现状分析

![image.png](https://upload-images.jianshu.io/upload_images/5959612-656f49d67768ec17.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

如何解决我们的数据从其他的server上移动到Hadoop之上？？？

> shell cp hadoop集群的机器上， hadoop fs -put ..... /



![业务现状分析](https://img-blog.csdn.net/20180325191845104?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3NodWp1ZWxpbg==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70) 



##3-3 -Flume概述

- Flume官网：http://flume.apache.org

- Flume是由Cloudera提供的一个分布式、高可靠、高可用的服务，用于分布式的海量日志的高效收集、聚合、移动系统

- Flume设计目标：

  可靠性
  扩展性
  管理性

- 业界同类产品的对比
  **Flume： Cloudera/Apache   Java**  

  Scribe： Facebook   C/C++   不再维护

  Chukwa： Yahoo/Apache  Java  不再维护
  Kafka：
  Fluentd： Ruby
  **Logstash: ELK(ElasticSearch,Kibana)**

- Flume发展史
  	Cloudera   0.9.2   **Flume-OG**
  	flume-728  **Flume-NG**  ==> Apache 
  	2012.7  1.0
  	2015.5  1.6   (*** + )
  	~		1.7



**Flume概述**：日志收集
Flume is a distributed, reliable,  （分布式、高可靠）
and available service for efficiently collecting(收集), 
aggregating(聚合), and moving(移动) large amounts of log data



**webserver(源端)  ===>  flume   ===> hdfs(目的地)**



**Flume架构**

![Flume架构](https://upload-images.jianshu.io/upload_images/5959612-b72fddf2ae9db3ed.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)









##3-4 -Flume架构及核心组件

Flume架构及核心组件
1) Source   收集  

2) Channel  聚集

3) Sink     输出



**设置多代理流程：Setting multi-agent flow**

![Two agents communicating over Avro RPC](http://flume.apache.org/_images/UserGuide_image03.png) 



**Consolidation：合并**

![A fan-in flow using Avro RPC to consolidate events in one place](http://flume.apache.org/_images/UserGuide_image02.png) 



**Multiplexing the flow：复用多个目的源**

![A fan-out flow using a (multiplexing) channel selector](http://flume.apache.org/_images/UserGuide_image01.png) 



##3-5 -Flume&JDK环境部署

**Flume安装前置条件**
    Java Runtime Environment - **Java 1.8** or later
    **Memory** - Sufficient memory for configurations used by sources, channels or sinks
    **Disk Space** - Sufficient disk space for configurations used by channels or sinks
    **Directory Permissions** - Read/Write permissions for directories used by agent



**安装jdk**
	下载
	解压到~/app
	将java配置系统环境变量中: ~/.bash_profile	
		export JAVA_HOME=/home/hadoop/app/jdk1.8.0_144
		export PATH=$JAVA_HOME/bin:$PATH
	source下让其配置生效
	检测: java  -version



**安装Flume**
	下载
	解压到~/app
	将java配置系统环境变量中: ~/.bash_profile	

```shell
export FLUME_HOME=/home/hadoop/app/apache-flume-1.6.0-cdh5.7.0-bin
export PATH=$FLUME_HOME/bin:$PATH
```

​	source下让其配置生效	
	flume-env.sh的配置：export JAVA_HOME=/home/hadoop/app/jdk1.8.0_144
	检测: flume-ng version



**安装Flume**
	下载
	解压到~/app
	将java配置系统环境变量中: ~/.bash_profile	

```shell
export FLUME_HOME=/home/hadoop/app/apache-flume-1.6.0-cdh5.7.0-bin
export PATH=$FLUME_HOME/bin:$PATH	
```

​	source下让其配置生效	
	flume-env.sh的配置：export JAVA_HOME=/home/hadoop/app/jdk1.8.0_144
	检测: flume-ng version



##3-6 -Flume实战案例一

- 需求：从指定网络端口采集数据输出到控制台

example.conf: A single-node Flume configuration

**使用Flume的关键就是写配置文件**

A） 配置Source
B） 配置Channel
C） 配置Sink
D） 把以上三个组件串起来

**a1: agent名称** 

**r1: source的名称**

**k1: sink的名称**

**c1: channel的名称**



**example.conf**

```shell
# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source  
# http://flume.apache.org/FlumeUserGuide.html#netcat-udp-source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444

# Describe the sink
# http://flume.apache.org/FlumeUserGuide.html#logger-sink
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory
# http://flume.apache.org/FlumeUserGuide.html#memory-channel
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```



**启动agent**

```shell
#官网启动：
$ bin/flume-ng agent -n $agent_name -c conf -f conf/flume-conf.properties.template

$ flume-ng agent \
--name a1  \
--conf $FLUME_HOME/conf  \
--conf-file $FLUME_HOME/conf/example.conf \
-Dflume.root.logger=INFO,console

$ flume-ng agent --name a1  --conf $FLUME_HOME/conf  --conf-file $FLUME_HOME/conf/example.conf -Dflume.root.logger=INFO,console
```



**使用telnet进行测试：** 

```shell
telnet hadoop000 44444
```



**Event分析**

Event: { headers:{} body: 68 65 6C 6C 6F 0D hello. }
Event是FLume数据传输的基本单元
Event =  可选的header + byte array



##3-7 -Flume实战案例二

- 监控一个文件实时采集新增的数据输出到控制台

需求二：
Agent选型：**exec source** + **memory channel** + **logger sink**



**exec-memory-logger.conf文件**

```shell
# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = exec
a1.sources.r1.command = tail -F /home/hadoop/data/data.log
a1.sources.r1.shell = /bin/sh -c

# Describe the sink
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory
a1.channels.c1.type = memory

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```



**启动agent** 

```shell
flume-ng agent \
--name a1  \
--conf $FLUME_HOME/conf  \
--conf-file $FLUME_HOME/conf/exec-memory-logger.conf \
-Dflume.root.logger=INFO,console

flume-ng agent --name a1  --conf $FLUME_HOME/conf --conf-file $FLUME_HOME/conf/exec-memory-logger.conf -Dflume.root.logger=INFO,console
```



##3-8 -Flume实战案例三(重点掌握)

- 将A服务器上的日志实时采集到B服务器

![img](https://img-blog.csdn.net/20180328195700492?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3NodWp1ZWxpbg==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70) 

需求三：
技术选型：

**exec**  source + **memory** channel + **avro** sink
**avro** source + **memory** channel + **logger** sink



**exec-memory-avro.conf**

```shell
exec-memory-avro.sources = exec-source
exec-memory-avro.sinks = avro-sink
exec-memory-avro.channels = memory-channel

exec-memory-avro.sources.exec-source.type = exec
exec-memory-avro.sources.exec-source.command = tail -F /home/hadoop/data/data.log
exec-memory-avro.sources.exec-source.shell = /bin/sh -c

exec-memory-avro.sinks.avro-sink.type = avro
exec-memory-avro.sinks.avro-sink.hostname = hadoop000
exec-memory-avro.sinks.avro-sink.port = 44444

exec-memory-avro.channels.memory-channel.type = memory

exec-memory-avro.sources.exec-source.channels = memory-channel
exec-memory-avro.sinks.avro-sink.channel = memory-channel
```

**avro-memory-logger.conf**

```shell
avro-memory-logger.sources = avro-source
avro-memory-logger.sinks = logger-sink
avro-memory-logger.channels = memory-channel

avro-memory-logger.sources.avro-source.type = avro
avro-memory-logger.sources.avro-source.bind = hadoop000
avro-memory-logger.sources.avro-source.port = 44444

avro-memory-logger.sinks.logger-sink.type = logger

avro-memory-logger.channels.memory-channel.type = memory

avro-memory-logger.sources.avro-source.channels = memory-channel
avro-memory-logger.sinks.logger-sink.channel = memory-channel
```



**先启动avro-memory-logger**

```shell
flume-ng agent \
--name avro-memory-logger  \
--conf $FLUME_HOME/conf  \
--conf-file $FLUME_HOME/conf/avro-memory-logger.conf \
-Dflume.root.logger=INFO,console

flume-ng agent \
--name exec-memory-avro  \
--conf $FLUME_HOME/conf  \
--conf-file $FLUME_HOME/conf/exec-memory-avro.conf \
-Dflume.root.logger=INFO,console
```



---

**Boy-20180610**







