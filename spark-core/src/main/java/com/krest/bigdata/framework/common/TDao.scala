package com.krest.bigdata.framework.common

import org.apache.spark.rdd.RDD

trait TDao {
  def readFile(path: String): RDD[String]
}
