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

    var Movie_rdd = sc.textFile(movies)

    val header = Movie_rdd.first()

    Movie_rdd = Movie_rdd.filter(row => row != header)

    val MovieDrama_result = Movie_rdd.map(row => row.split(','))
      .map(fields => (fields(1), fields(2)))
      .flatMapValues(x => x.split('|'))
      .filter(x => x._2 == "Action")

    println(" Movies with the Drama genre are   :" + MovieDrama_result.count())


  }
}
