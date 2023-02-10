package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_partitionBy {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // partitionBy
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))
    // RDD => PairRDDFunctions
    // 隐式转换（二次编译）

    // partitionBy 根据指定的分区规则对数据进行重分区
    val newRDD = mapRDD.partitionBy(new HashPartitioner(2))
  
    newRDD.saveAsTextFile("data/output")


    sc.stop()

  }
}
