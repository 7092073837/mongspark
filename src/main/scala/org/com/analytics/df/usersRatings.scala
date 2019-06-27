package org.com.analytics.df

import org.com.analytics.df.loadSourceData.load_ratings
import spark.spark_session

object usersRatings  {

  def userMostRatings(): Unit = {


    val ratingsDF = load_ratings.createOrReplaceTempView("ratings_tbl")
    val usersWithRatingsDF = spark_session.sql("select userId , count(rating) as most_ratings from" +
      " ratings_tbl group by userId order by most_ratings desc")

    println("Users rating sorted : ")
    usersWithRatingsDF.show(20)
  }


}