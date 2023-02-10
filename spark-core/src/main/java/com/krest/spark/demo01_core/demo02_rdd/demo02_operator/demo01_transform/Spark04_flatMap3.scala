package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_flatMap3 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //  flatMap
    val rdd = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))

    val flatRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case temp => List(temp)
        }
      }
    )
    flatRDD.collect().foreach(println)


    sc.stop()

  }
}
