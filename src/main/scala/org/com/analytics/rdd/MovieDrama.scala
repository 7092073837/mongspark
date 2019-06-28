package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**
  * loaded Movie datasets
  *
  *  Extract the first row which is the header
  * Filter out the header from the dataset
  * Split the Input split & Make some split on Genre Based on Action
  *
  */
object MovieDrama  extends App with Context{

  def movieDrama(): Unit = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    var movieRDD = sc.textFile(movies)

    val movieHeader = movieRDD.first()

    movieRDD = movieRDD.filter(row => row != movieHeader).filter(line=>line.contains("Drama"))

    println("Count of movies with the Drama genre are   :" + movieRDD.count())


  }
}
