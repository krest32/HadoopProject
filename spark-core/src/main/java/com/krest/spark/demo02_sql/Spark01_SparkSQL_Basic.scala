package com.krest.spark.demo02_sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {

  def main(args: Array[String]): Unit = {

    // 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    // 执行逻辑操作
    //  DataFrame
    val df: DataFrame = spark.read.json("data/input/user.json")
    df.show()
    println("+++++++++++++++++++++++++++")


    // DataFrame => SQL
    // 创建一个临时的视图，只能查不能改
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show
    spark.sql("select age, username from user").show
    spark.sql("select avg(age) from user").show
    println("+++++++++++++++++++++++++++")


    // DataFrame => DSL
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则，转换为其他列的名称
    import spark.implicits._
    df.select("age", "username").show
    df.select($"age" + 1).show
    df.select('age + 1).show
    println("+++++++++++++++++++++++++++")

    // DataSet
    // DataFrame其实是特定泛型的DataSet
    val seq = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()
    println("+++++++++++++++++++++++++++")


    // 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
