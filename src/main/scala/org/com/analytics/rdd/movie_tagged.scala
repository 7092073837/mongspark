package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**
  * loaded tag & Rating datasets
  *
  *  Extract the first row which is the header
  * Filter out the header from the dataset
  * Split the Input split & Make some split on Genre Based on Action
  *
  */
object movie_tagged  extends App with Context  {

    case class Rating(userid: Int, movieid: Int, rating: Double, timestamp: BigInt)
    case class Tags(userid: Int, movieid: Int, tag: String, timestamp: BigInt)

    def movietagged(): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)

        val sqlContext = new org.apache.spark.sql.SQLContext (sc)
        import sqlContext.implicits._
        val ratingsRDD = sc.textFile(ratings)
        val ratingsHeader = ratingsRDD.first();
        val ratingsWOHeaderRDD = ratingsRDD.filter(row => row != ratingsHeader)

        val ratingsDF = ratingsWOHeaderRDD.map(_.split(",")).map(p=>Rating(p(0).toInt,p(1).toInt,p(2).toDouble,p(3).toLong)).toDF
        ratingsDF.registerTempTable("ratings")

        val tagsRDD = sc.textFile(tags)
        val tagsHeaders = tagsRDD.first();
        val tagsWOHeaderRDD = tagsRDD.filter(row => row != tagsHeaders)

        val tagsDF = tagsWOHeaderRDD.map(_.split(",")).map(p=>Tags(p(0).toInt,p(1).toInt,p(2),p(3).toLong)).toDF
        tagsDF.registerTempTable("tags")


        sqlContext.sql("select count(distinct r.userid) as user_count_wo_tag from ratings r join tags t where r.userid=t.userid and r.movieid=t.movieid").show


        sqlContext.sql("select count(distinct(r.userid)) as user_with_rating from ratings r left join tags t where r.userid=t.userid and r.movieid not in (select movieid from tags)").show






    }
}
