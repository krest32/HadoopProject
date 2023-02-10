package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_groupBy2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // groupBy
    val rdd = sc.makeRDD(List("Hello", "Spark", "Scala", "Hadoop"), 2)

    // 分组和分区没有必然的关系
    val groupRDD = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)


    sc.stop()

  }
}
