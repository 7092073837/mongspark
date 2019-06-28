package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**
  * Extract the first row which is the header
  *  Filter out the header from the dataset
  *  Split the Input split & Make some split on Findout average movie
  *   Print each result on its own line.
  */
object average_movies extends App with Context {

  case class Rating(userid: Int, movieid: Int, rating: Double, timestamp: BigInt)
  def average_movie() {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    import sqlContext.implicits._
    val ratingsRDD = sc.textFile(ratings)
    val ratingsHeader = ratingsRDD.first();
    val ratingsWOHeaderRDD = ratingsRDD.filter(row => row != ratingsHeader)
    val ratingsDF = ratingsWOHeaderRDD.map(_.split(",")).map(p=>Rating(p(0).toInt,p(1).toInt,p(2).toDouble,p(3).toLong)).toDF
    ratingsDF.registerTempTable("ratings")
    sqlContext.sql("select movieid,min(rating),max(rating),avg(rating) from ratings group by movieid order by movieid asc").show
  }}