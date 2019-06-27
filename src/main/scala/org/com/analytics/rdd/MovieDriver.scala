package org.com.analytics.rdd

import org.com.analytics.rdd.MovieDrama.movieDrama
import org.com.analytics.rdd.MovieRatings.movierating
import org.com.analytics.rdd.average_movies.average_movie
import org.com.analytics.rdd.genre_per_year.genreperyear
import org.com.analytics.rdd.movie_tagged.movietagged
import org.com.analytics.rdd.release_year.releaseyear

object MovieDriver {
def main(args:Array[String]): Unit ={


       movieDrama()
      movierating()
       movietagged()
      releaseyear()
       genreperyear()
       average_movie()
}
}
