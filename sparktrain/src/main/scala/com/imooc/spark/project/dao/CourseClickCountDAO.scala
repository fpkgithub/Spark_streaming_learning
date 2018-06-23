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