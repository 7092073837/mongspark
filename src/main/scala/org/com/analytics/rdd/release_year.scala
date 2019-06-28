package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}


/**Read a movies text file
  *Filter out the header from the dataset
  *Slice the data and
  * generate result
  */
object release_year extends App with Context {
  case class Movies(movieId: Int,title: String,genres:String)
  def releaseyear() {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    import sqlContext.implicits._
    val moviesRDD = sc.textFile(movies)
    val moviesHeader = moviesRDD.first()
    val moviesWOHeaderRDD = moviesRDD.filter(row => row != moviesHeader)


    val moviesDF = moviesWOHeaderRDD.map(_.split(",")).map(p=>Movies(p(0).toInt,p(1),p(2))).toDF
    moviesDF.registerTempTable("movies")

    sqlContext.sql("select movieId,title,genres,REGEXP_REPLACE(substr(title,length(title)-6,7),'[(]|[)]','') as year from movies limit 10").show


  }




}
