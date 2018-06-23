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