package org.com.analytics.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait Context {
  lazy val spark = SparkSession.builder.master("local[2]").appName("Sparkmovie").getOrCreate

  val ssc =new StreamingContext(spark.sparkContext,Seconds(10))

  lazy val sc =spark.sparkContext

  lazy val movies = "src/main/input/movies.csv"
  lazy val tags = "src/main/input/tags.csv"
 lazy  val ratings ="src/main/input/ratings.csv"
}
