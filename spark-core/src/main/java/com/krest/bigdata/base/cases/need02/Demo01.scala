package com.krest.bigdata.base.cases.need02

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("hot top 10")
    val sc: SparkContext = new SparkContext(sparkConf)

    val actionRdd: RDD[String] = sc.textFile("CoreCaseData")
    val topCids: Array[String] = top10cateGory(actionRdd)


    // 1. 国旅原始数据
    val filterActionRdd = actionRdd.filter(
      line => {
        val data = line.split("_")
        if (data(6) != "-1") {
          topCids.contains(data(6))
        } else {
          false
        }
      }
    )


    val reduceRdd: RDD[((String, String), Int)] = filterActionRdd.map(
      action => {
        val data = action.split("_")
        ((data(6), data(2)), 1)
      }
    ).reduceByKey(_ + _)

    // 将统计的结果进行转化
    val mapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    // 相同的品类进行分组
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()

    // 分组后的数据进行点击量的排序
    val result: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      it => {
        it.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    // 打印结果
    result.collect().foreach(println)

    sc.stop()
  }

  def top10cateGory(actionRdd: RDD[String]): Array[String] = {
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
    ansRdd.sortBy(_._2, false).take(10).map(_._1)
  }
}
