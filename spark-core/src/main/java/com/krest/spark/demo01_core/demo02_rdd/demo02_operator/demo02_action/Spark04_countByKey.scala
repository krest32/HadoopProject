package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo02_action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_countByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        //val rdd = sc.makeRDD(List(1,1,1,4),2)
        val rdd = sc.makeRDD(List(
            ("a", 1),("a", 2),("a", 3)
        ))

        // countByKey

       //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        //println(intToLong)
        val stringToLong: collection.Map[String, Long] = rdd.countByKey()
        println(stringToLong)

        sc.stop()

    }
}
