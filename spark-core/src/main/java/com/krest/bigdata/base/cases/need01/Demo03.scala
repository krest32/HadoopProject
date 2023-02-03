package com.krest.bigdata.base.cases.need01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 针对 demo02 问题
 * 1. 存在大量的 reduceBykey，会产生大量的 shuffle 操作，性能也会有影响
 * spark 针对相同的数据源进行 shuffle 操作优化，但是这个项目中存在多种不同的数据源
 */
object Demo03 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hot top 10")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取日志数据
    val actionRdd: RDD[String] = sc.textFile("CoreCaseData")

    // 统计品类的点击数量
    val flatRdd: RDD[(String, (Int, Int, Int))] = actionRdd.flatMap(line => {
      val data = line.split("_")
      if (data(6) != "-1") {
        List((data(6), (1, 0, 0)))
      } else if (data(8) != "null") {
        val cids = data(8).split(",")
        cids.map(cid => {
          (cid, (0, 1, 0))
        })
      } else if (data(10) != "null") {
        val cids = data(10).split(",")
        cids.map(cid => {
          (cid, (0, 0, 1))
        })
      } else {
        Nil
      }
    })

    val ansRdd: RDD[(String, (Int, Int, Int))] = flatRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    // 降序排序，取前十
    val result = ansRdd.sortBy(_._2, false).take(10)

    // 将结果进行打印
    result.foreach(println)


    sc.stop()
  }
}
