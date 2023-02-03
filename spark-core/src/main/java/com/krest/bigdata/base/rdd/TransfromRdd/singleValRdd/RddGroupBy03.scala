package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * groupby 根据数据进行分组，注意分组和分区没有必然的关系
 */
object RddGroupBy03 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    //    一行一行的拿取数据
    val rdd: RDD[String] = sc.textFile("data/apache.log")

    // 分组判断 : 统计每个时间段的数据
    val timeRdd: RDD[(Int, Iterable[(Int, Int)])] = rdd.map(line => {
      val datas = line.split(" ")
      val time = datas(3)
      val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val date = sdf.parse(time)
      val hours = date.getHours
      //      每来一行，就有1次
      (hours, 1)
    }).groupBy(_._1)

    timeRdd.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect.foreach(println)

    sc.stop()

  }
}
