package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**
  * Extract the first row which is the header
  *  Filter out the header from the dataset
  *  Split the Input split & Make some split on Findout average movie
  *   Print each result on its own line.
  */
object average_movies extends App with Context {


  def average_movie() {

    Logger.getLogger("org").setLevel(Level.ERROR)


    def mapToTuple(line: String): (Int, (Float, Int)) = {
      val fields = line.split(',')
      return (fields(1).toInt, (fields(2).toFloat, 1))
    }

    var rating_rdd = sc.textFile(ratings)

    rating_rdd.min()
    val header = rating_rdd.first();
val i =5
    val ie = 10
    rating_rdd = rating_rdd.filter(row => row != header)
    val result = rating_rdd.map(mapToTuple)

    val result1 = rating_rdd.map(mapToTuple)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val result3 = rating_rdd.map(mapToTuple)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .sortBy(_._2, false)
      .collect()

    val ratings23 = rating_rdd.map(x => x.toString().split(",")(2))

    val results = ratings23.countByValue()

    val sortedResults = results.toSeq.sortBy(_._1)


    print("average movie per ratings" + sortedResults.take(1).foreach(println))
    print("min rating per movie "+ sortedResults.take(i).foreach(println))
print("max rating per movie " + sortedResults.take(ie).foreach(println))
  }}