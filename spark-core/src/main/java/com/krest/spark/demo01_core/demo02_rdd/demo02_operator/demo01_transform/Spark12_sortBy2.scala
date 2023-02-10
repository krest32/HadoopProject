package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_sortBy2 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

      //  sortBy
      val rdd: RDD[(String, Int)] = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)), 2)

    // sortBy 方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
    // sortBy 默认情况下，不会改变分区。但是中间存在shuffle操作
    val newRDD = rdd.sortBy(t => t._1.toInt, false)

    newRDD.collect().foreach(println)


    sc.stop()

  }
}
