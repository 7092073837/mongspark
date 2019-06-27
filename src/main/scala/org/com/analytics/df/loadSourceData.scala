package org.com.analytics.df

import spark.spark_session

object loadSourceData {

  val  load_movie = spark_session.read.option("header","true")
    .option("inferSchema","true").option("delimiter", ",").option("encoding", "windows-1252")
    .csv("src/main/input/movies.csv")

  val load_ratings = spark_session.read.option("header", "true")
    .option("inferSchema", "true").option("delimiter", ",").option("encoding", "windows-1252")
    .csv("src/main/input/ratings.csv")

  val load_tags = spark_session.read.option("header", "true")
    .option("inferSchema", "true").option("delimiter", ",").option("encoding", "windows-1252")
    .csv("src/main/input/tags.csv")


}