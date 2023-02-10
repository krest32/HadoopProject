package com.krest.spark.demo01_core.demo02_rdd.demo02_operator.demo01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_glom2 {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        // glom 求取每个分区的最大值，然后进行求和
        val rdd : RDD[Int] = sc.makeRDD(List(1,2,3,4), 2)

        // 【1，2】，【3，4】
        // 【2】，【4】
        // 【6】
        val glomRDD: RDD[Array[Int]] = rdd.glom()

        val maxRDD: RDD[Int] = glomRDD.map(
            array => {
                array.max
            }
        )

        println(maxRDD.collect().sum)





        sc.stop()

    }
}
