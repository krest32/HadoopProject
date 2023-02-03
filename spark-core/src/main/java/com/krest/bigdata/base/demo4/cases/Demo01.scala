package com.krest.bigdata.base.demo4.cases

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * Rdd 小练习
 * 统计每个省份每个广告被点击数量排行的 Top3
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")

    val sc = new SparkContext(sparkConf)

    //    一行一行的拿取数据
    val rdd: RDD[String] = sc.textFile("data/agent.log")
    val mapRdd: RDD[((String, String), Int)] = rdd.map(line => {
      val data = line.split(" ")
      ((data(1), data(4)), 1)
    })

    val reduceRdd: RDD[((String, String), Int)] = mapRdd.reduceByKey(_ + _)

    val newMapRdd = reduceRdd.map {
      case ((pre, ad), sum) => {
        (pre, (ad, sum))
      }
    }

    val groupRdd: RDD[(String, Iterable[(String, Int)])] = newMapRdd.groupByKey()
    val ansRdd = groupRdd.mapValues(iter => {
      // 默认升序排序，这里修改为降序，同时需前三
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })

    ansRdd.collect().foreach(println)
  }
}
