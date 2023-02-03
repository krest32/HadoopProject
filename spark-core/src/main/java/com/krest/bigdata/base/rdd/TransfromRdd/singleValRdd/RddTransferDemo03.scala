package com.krest.bigdata.base.rdd.TransfromRdd.singleValRdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 体现数据处理的并行
 */
object RddTransferDemo03 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)

    // 多个数据并行处理
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    //    map 功能强大，但是性能不高
    //    mapPartitions 性能更高，类似于一次性拿到了一部分数据，集体进行操作，内部是个迭代器
    //    mapPartitions 是它会将整个分区的数据放在内存中，不会被释放，如果内存小，数据量大，可能会导致内存溢出
    val mapRdd1 = rdd.mapPartitions(iter => {
      println(">>>>>>>>>>>>>>>>")
      iter.map(_ * 2)
    })

    mapRdd1.collect().foreach(println)
    sc.stop()
  }
}
