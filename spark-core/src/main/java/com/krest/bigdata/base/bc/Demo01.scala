package com.krest.bigdata.base.bc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 广播器
 */
object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))




    //    val rdd2 = sc.makeRDD(List(
    //      ("a", 4), ("b", 5), ("c", 6)
    //    ))

    // join 会导致 数据量几何倍增加，并且会影响 shuffle 的性能，不推荐使用
    // val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    // rdd3.collect().foreach(println)

    /**
     * 改进一 解决了 shuffle 性能的问题，数据量几何倍增加的问题
     * 但是如果数据量很多，会导致数据传输的问题，或者数据冗余的问题
     */
    //    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    //    rdd1.map {
    //      case (w, c) => {
    //        val num: Int = map.getOrElse(w, 0)
    //        (w, (c, num))
    //      }
    //    }.collect().foreach(println)


    /**
     * 改进二：使用广播变量
     * 1. 数据被共享
     * 2. 数据不能被修改
     */
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    // 封转广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map {
      case (w, c) => {
        val num: Int = bc.value.getOrElse(w, 0)
        (w, (c, num))
      }
    }.collect().foreach(println)


    sc.stop()
  }
}
