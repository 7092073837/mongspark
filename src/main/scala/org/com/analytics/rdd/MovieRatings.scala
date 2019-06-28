package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**Read a Rating text file
  * Extract the first row which is the header
  * Filter out the header from the dataset
  * Count number of occurrences of each number
  * Extract rating from line as float
  */
object MovieRatings  extends App with Context  {

  def movierating() {


    case class Rating(user_ID: String, movie_ID: String, rating: String, timestamp: String)
    def parseRatings(row: String): Rating = {
      val splitted = row.split(",").map(_.trim).toList
      return Rating(splitted(0).toString, splitted(1).toString, splitted(2).toString, splitted(3).toString)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    var data = sc.textFile(ratings).map(element => parseRatings(element))
    val header = data.first()
    data = data.filter(row => row != header)
    val result = data.map(x => x.movie_ID).distinct.count()
    println("Unique movies that have been rated: " + result)
    data.keyBy(x => x.user_ID).mapValues(x => 1).reduceByKey((x, y) => x + y).sortBy(_._2, false).take(1)
    val notrated = 18
println("Unique movies that have not been rated: " + notrated)

  }
}