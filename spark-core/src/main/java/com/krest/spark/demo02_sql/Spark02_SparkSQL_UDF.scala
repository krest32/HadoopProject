package com.krest.spark.demo02_sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SparkSQL_UDF {

  def main(args: Array[String]): Unit = {

    // 创建 SparkSQL 的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("data/input/user.json")
    // 创建 user 视图
    df.createOrReplaceTempView("user")

    // 注册 udf 自定义函数：在 name 的前面加个前缀
    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })

    spark.sql("select age, prefixName(username) from user").show


    // TODO 关闭环境
    spark.close()
  }
}
