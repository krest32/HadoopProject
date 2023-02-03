package com.krest.bigdata.base.cases.need01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 解析用户行为数据
 * 需求1：top10 热门商品
 * 根据商品的点击、下单、支付的量来统计热门的品类
 * 1.1 先按照点击数
 * 1.2 再按照下来数
 * 1.3 最后按照支付数
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hot top 10")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 读取日志数据
    val actionRdd: RDD[String] = sc.textFile("CoreCaseData")

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
    val coGroupRdd: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCntRdd.cogroup(orderCntRdd, payCntRdd)

    // 对数据进行取值
    val ansRdd: RDD[(String, (Int, Int, Int))] = coGroupRdd.mapValues {
      case (clickIt, orderIt, payIt) => {
        var clickCnt = 0;
        var orderCnt = 0;
        var payCnt = 0;
        if (clickIt.iterator.hasNext) {
          clickCnt = clickIt.iterator.next()
        }
        if (orderIt.iterator.hasNext) {
          orderCnt = orderIt.iterator.next()
        }
        if (payIt.iterator.hasNext) {
          payCnt = payIt.iterator.next()
        }
        (clickCnt, orderCnt, payCnt)
      }
    }

    // 降序排序，取前十
    val result = ansRdd.sortBy(_._2, false).take(10)


    // 将结果进行打印
    result.foreach(println)


    sc.stop()
  }
}
