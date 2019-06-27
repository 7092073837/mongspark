package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**
  *Loaded  data into rdd for tag rdd
  * join with rating rdd
  *based on user are taged in rating
  */
object genre_per_year  extends App with Context  {
  def genreperyear() {
    Logger.getLogger("org").setLevel(Level.ERROR)


    var movies_rdd = sc.textFile(movies)

    val header = movies_rdd.first()
    movies_rdd = movies_rdd.filter(row => row != header)
    val genre_per_year_result = movies_rdd.map(row => row.split(','))
      .map(fields => (fields(0) + "," + fields(1), fields(2)))
      .flatMapValues(x => x.split('|'))
      .collect()
    genre_per_year_result.take(10000).foreach(println)

  }

}
