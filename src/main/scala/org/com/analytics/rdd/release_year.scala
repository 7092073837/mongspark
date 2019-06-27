package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**Read a movies text file
  *Filter out the header from the dataset
  *Slice the data and
  * generate result
  */
object release_year extends App with Context {

  def releaseyear() {



    Logger.getLogger("org").setLevel(Level.ERROR)

    var release_year_rdd = sc.textFile(movies)

    val header = release_year_rdd.first()
    release_year_rdd = release_year_rdd.filter(row => row != header)

    val fil = release_year_rdd.map(row => row.split(',')(1).toString()
    )

    val sv = fil.map(x => x.slice(0, x.length - 6) ++ "," ++ x.slice(x.length - 5, x.length - 1))


    sv.take(10000).foreach(println)


  }




}
