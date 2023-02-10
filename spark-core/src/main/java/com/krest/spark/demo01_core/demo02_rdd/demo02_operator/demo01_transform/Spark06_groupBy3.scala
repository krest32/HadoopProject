package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从服务器日志数据 apache.log 中获取每个时间段访问量
 * 1. 数据类型：
 *  83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
 * 2. 根据空格获取数据的时间信息
 * 3. 格式化时间信息，获取小时
 * 4. 统计每个小时的访问数量
 *
 */
object Spark06_groupBy3 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // groupBy
    val rdd = sc.textFile("data/apache.log")

    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val datas = line.split(" ")
        val time = datas(3)
        //time.substring(0, )
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        (hour, 1)
      }
    ).groupBy(_._1)


    // todo 增加一个按照时间排序的功能
    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.sortBy(data => data._1, ascending = true)
      .collect
      .foreach(println)


    sc.stop()

  }
}
