package org.com.analytics.rdd

import org.com.analytics.rdd.MovieDrama.movieDrama
import org.com.analytics.rdd.MovieRatings.movierating
import org.com.analytics.rdd.average_movies.average_movie
import org.com.analytics.rdd.genre_per_year.genreperyear
import org.com.analytics.rdd.movie_tagged.movietagged
import org.com.analytics.rdd.release_year.releaseyear

object MovieDriver {
def main(args:Array[String]): Unit ={

  /**
    * Below set of functions are answers for General questions
    */
       movieDrama()//#1. How many “Drama” movies (movies with the "Drama" genre) are there?
       movierating()//#2. How many unique movies are rated, how many are not rated?
       mostRatedByUser.mostRated()//#3. Who give the most ratings, how many rates did he make?
       average_movie()//#4. Compute min, average, max rating per movie.
       movietagged()//#5.Output dataset containing users that have rated a movie but not tagged it.  #6.Output dataset containing users that have rated AND tagged a movie.
       //#7. As per readme, movie titles are entered manually or imported from <https://www.themoviedb.org/>, and include the year of release in parentheses.
       releaseyear()//#8. Enrich movies dataset with extract the release year. Output the enriched dataset.
       genreperyear()//#9.Output dataset showing the number of movies per Genre per Year

  /**
    * Below set of functions are answers for QA specific question
    */



}
}
