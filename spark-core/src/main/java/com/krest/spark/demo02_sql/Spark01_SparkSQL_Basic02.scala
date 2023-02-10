package com.krest.spark.demo02_sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic02 {

  def main(args: Array[String]): Unit = {


    // 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // RDD <=> DataFrame
    val rdd1 = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd1.toDF("id", "name", "age")
    df.show()
    val rowRDD: RDD[Row] = df.rdd

    // DataFrame <=> DataSet
    val ds: Dataset[User] = df.as[User]
    val df1: DataFrame = ds.toDF()

    // RDD <=> DataSet
    val ds1: Dataset[User] = rdd1.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD: RDD[User] = ds1.rdd

    spark.close()
  }

  case class User(id: Int, name: String, age: Int)

}
