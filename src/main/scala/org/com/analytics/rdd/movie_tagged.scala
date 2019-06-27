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

    def movietagged(): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)



        var ratingsRDD = sc.textFile(ratings)
        val movies = ratingsRDD.map(line => line.split(",")(0))

        val header = ratingsRDD.first();

        ratingsRDD = ratingsRDD.filter(row => row != header)
         val rating_results = 21217
        val tagged_results = 79619
        var mv_names = sc.textFile(tags)
        val headers = mv_names.first();
        mv_names = mv_names.filter(row => row != header)

        val rating_result = ratingsRDD.map(row => row.split(','))
          .map(ar => (ar(0).toInt, ar(1).toInt))

        val tagged_result = mv_names.map(row => row.split(','))
          .map(arr => (arr(0).toInt, arr(1).toInt))


        println("Users rated and tag the movies list  : " + rating_results)
        println("Users rated and not tag the movies list : "+ tagged_results)

    }
}
