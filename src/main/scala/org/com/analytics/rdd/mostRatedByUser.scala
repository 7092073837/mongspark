package org.com.analytics.rdd



object mostRatedByUser extends App with Context{

  /**
    * loaded Movie datasets
    *
    *  Extract the first row which is the header
    * Filter out the header from the dataset
    * Split the Input split & Make some split on Genre Based on Action
    *
    */

  case class Rating(userid: Int, movieid: Int, rating: Double, timestamp: BigInt)
     def mostRated(): Unit = {
       val sqlContext = new org.apache.spark.sql.SQLContext (sc)
       import sqlContext.implicits._
       val ratingsRDD = sc.textFile(ratings)
       val ratingsHeader = ratingsRDD.first
       val ratingsWOHeaderRDD = ratingsRDD.filter(row => row != ratingsHeader)
       val ratingsDF = ratingsWOHeaderRDD.map(_.split(",")).map(p=>Rating(p(0).toInt,p(1).toInt,p(2).toDouble,p(3).toLong)).toDF
       ratingsDF.registerTempTable("ratings")
       sqlContext.sql("select userId , count(rating) as most_ratings from ratings group by userId order by most_ratings desc limit 1").show

     }
}
