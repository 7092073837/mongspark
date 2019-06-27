package org.com.analytics.df

import org.com.analytics.df.loadSourceData.{load_ratings, load_tags}
import spark.spark_session
object MoviesRatedTagged {
  def movieRatedTags() {


    val tagsMoviesDF  = load_tags.createOrReplaceTempView("tag_movies_tbl")
    val ratedMoviesDF = load_ratings.createOrReplaceTempView("rating_movies_tbl")


    val joinedRatedTagsDF = spark_session.sql("select * from rating_movies_tbl where userId in (select userId from tag_movies_tbl )")
    println("Users rated and tag the movies list  :  "+joinedRatedTagsDF.count())

    val joinedRatedWithoutTagsDF = spark_session.sql("select * from rating_movies_tbl where userId not in (select userId from tag_movies_tbl )")
    println("Users rated and not tag the movies list :  "+joinedRatedWithoutTagsDF.count())


  }

}