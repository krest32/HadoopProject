package com.krest.spark.core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_mapPartitionsWithIndex2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //  mapPartitionsWithIndex
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val mpiRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        // 1,   2,    3,   4
        //(0,1)(2,2),(4,3),(6,4)
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )

    mpiRDD.collect().foreach(println)


    sc.stop()

  }
}
