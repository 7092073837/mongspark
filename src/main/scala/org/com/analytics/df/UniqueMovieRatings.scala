package org.com.analytics.df

import org.com.analytics.df.loadSourceData.{load_movie, load_ratings}
import spark.spark_session
object UniqueMovieRatings  {

  def uniqueMovies() {


    val moviesDF = load_movie.createOrReplaceTempView("movie_tbl")
    val ratingsDF = load_ratings.createOrReplaceTempView("ratings_tbl")

    val uniqueMoviesRatedDF = spark_session.sql("select * from movie_tbl where movieId in (select movieId  from ratings_tbl ) ")

    println("Unique rated movies are :  "+uniqueMoviesRatedDF.count())

   val uniqueMoviesNotRatedDF = spark_session.sql("select * from movie_tbl where movieId not in (select movieId  from ratings_tbl ) ")

    println("Unique movies which are not rated :  "+uniqueMoviesNotRatedDF.count())


  }

}