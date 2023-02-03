package com.krest.bigdata.base.rdd.actionRdd.cases

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo-LogOperator")
    val sc = new SparkContext(sparkConf)
    //    一行一行的拿取数据
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User()

    // rdd 中传递函数会包含闭包操作，那么就会进行检测功能，所以被称为闭包检测
    rdd.foreach(num => {
      println("age = " + (user.age + num))
    })

    sc.stop()
  }

  class User extends Serializable {
    var age: Int = 30
  }

}
