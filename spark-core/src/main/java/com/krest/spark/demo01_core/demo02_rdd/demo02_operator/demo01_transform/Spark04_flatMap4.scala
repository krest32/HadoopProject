package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_flatMap4 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //  flatMap
    val rdd: RDD[String] = sc.makeRDD(List(
      "Hello Scala", "Hello Spark"
    ))

    val flatRDD: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )

    val mapRdd = flatRDD.map(data => Tuple2(data, 1))

    mapRdd.collect().foreach(println)

    sc.stop()

  }
}
