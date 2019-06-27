package org.com.analytics.df

import org.apache.log4j.{Level, Logger}

object MovieProcessEvent  {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    dramaMovies.filterDramaMovies()
    movieRatings.movieRating()
    UniqueMovieRatings.uniqueMovies()
    usersRatings.userMostRatings()
    MoviesRatedTagged.movieRatedTags()
    NumberOfMovies.numOfGenresPerYear()

  }
}