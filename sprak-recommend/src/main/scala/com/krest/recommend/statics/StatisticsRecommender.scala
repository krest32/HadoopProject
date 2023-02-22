package com.krest.recommend.statics

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/* 实体类 */
case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"
  // 历史热门统计
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  // 最近热门统计
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  // 优质商品统计
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[1]",
      "mongo.uri" -> "mongodb://192.168.160.139:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据，成为一张数据表的形式
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 创建一张叫ratings的临时表--类似一个视图
    ratingDF.createOrReplaceTempView("ratings")

    // 1. 历史热门商品，按照评分个数统计，productId，count
    val rateMoreProductsDF = spark.sql(
      "select productId, count(productId) as count from ratings group by productId order by count desc"
    )
    // 清除历史数据
    val collection1 = mongoClient(mongoConfig.db)(RATE_MORE_PRODUCTS)
    collection1.dropCollection()
    // 保存数据到 mongo
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 2. 近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 自定义注册 UDF 函数，将 timestamp(int类型的秒) 转化为年月格式  yyyyMM
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 把原始rating数据转换成想要的结构productId, score, yearmonth
    val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = spark.sql(
      "select productId, count(productId) as count, yearmonth from ratingOfMonth " +
        "group by yearmonth, productId " +
        "order by yearmonth desc, count desc"
    )
    // 清除历史数据
    val collection2 = mongoClient(mongoConfig.db)(RATE_MORE_RECENTLY_PRODUCTS)
    collection2.dropCollection()
    // 把df保存到mongodb
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    // 3. 优质商品统计，商品的平均评分，productId，avg
    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    // 清除历史数据
    val collection3 = mongoClient(mongoConfig.db)(AVERAGE_PRODUCTS)
    collection3.dropCollection()
    // 把df保存到mongodb
    storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {

    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
