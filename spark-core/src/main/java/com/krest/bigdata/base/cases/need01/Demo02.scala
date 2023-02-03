package com.krest.bigdata.base.cases.need01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 针对 demo01 问题
 * 1. actionRdd 被重复使用多次
 * 方案：使用存储
 * 2. coGroup 是否会有性能问题？ cogroup 在操作数据源的时候，如果分组不同会重新 shuffle 操作
 * 方案：一次性进行多种数据操作
 */
object Demo02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hot top 10")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取日志数据
    val actionRdd: RDD[String] = sc.textFile("CoreCaseData")
    actionRdd.cache()

    // 统计品类的点击数量
    val clickRdd: RDD[String] = actionRdd.filter(line => {
      val data = line.split("_")
      data(6) != "-1"
    })

    val clickCntRdd: RDD[(String, Int)] = clickRdd.map(line => {
      val data = line.split("_")
      // 拿到品类的id, 1表示点击一次
      (data(6), 1)
    }).reduceByKey(_ + _)

    // 统计品类的下单数量，下单数量是多个，所以需要使用 flatMap
    val orderRdd: RDD[String] = actionRdd.filter(line => {
      val data = line.split("_")
      data(8) != "null"
    })

    val orderCntRdd: RDD[(String, Int)] = orderRdd.flatMap(line => {
      val data: Array[String] = line.split("_")
      val cids: Array[String] = data(8).split(",")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)

    // 统计品类的支付数量
    val payRdd: RDD[String] = actionRdd.filter(line => {
      val data = line.split("_")
      data(10) != "null"
    })

    val payCntRdd: RDD[(String, Int)] = orderRdd.flatMap(line => {
      val data: Array[String] = line.split("_")
      val cids: Array[String] = data(10).split(",")
      cids.map(id => (id, 1))
    }).reduceByKey(_ + _)


    // 合并统计数据（coGroup） 对商品进行排序【点击->下单->支付】，取前10，可以采用元祖排序
    val rdd1 = clickCntRdd.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }
    val rdd2 = orderCntRdd.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }
    val rdd3 = payCntRdd.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }

    val sourceRdd: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val ansRdd: RDD[(String, (Int, Int, Int))] = sourceRdd.reduceByKey(
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
