package org.com.analytics.df

import org.com.analytics.df.loadSourceData.{load_ratings, load_tags}
import spark.spark_session

object movieRatings  {

  def movieRating(): Unit = {


    val ratingsDF = load_ratings.createOrReplaceTempView("ratings_tbl")

    val tagsDF = load_tags.createOrReplaceTempView("tags_tbl")

    val ratings_tags_join = spark_session.sql("select r.userId,r.movieId,tag,rating " +
      "from ratings_tbl r inner join tags_tbl t where r.movieId = t.movieId ")

    val movieRatingsDF = ratings_tags_join.createOrReplaceTempView("ratings_tags_tbl")


    val movieRatingsDFFinal = spark_session.sql("select distinct movieId,min(rating) over (partition by movieId )as min_rating, avg(rating)over (partition by movieId ) as avg_rating ," +
      " max(rating)over (partition by movieId ) as max_rating from ratings_tags_tbl ")

    println("The ratingsDF for movies are : ")
    movieRatingsDFFinal.show(10)

  }
}