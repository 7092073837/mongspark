package org.com.analytics.rdd

object test extends App with Context {

  val csv = sc.textFile(movies)
  csv.take(10).foreach(println)

}
