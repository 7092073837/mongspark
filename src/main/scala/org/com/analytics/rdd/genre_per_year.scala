package org.com.analytics.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.split


/**
  *Loaded  data into rdd for tag rdd
  * join with rating rdd
  *based on user are taged in rating
  */
object genre_per_year  extends App with Context  {
  def genreperyear() {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    import sqlContext.implicits._
    spark.read.csv(movies).toDF.registerTempTable("movies")
    sqlContext.sql("select `_c2` as genre,REGEXP_REPLACE(substr(`_c1`,length(`_c1`)-6,7),'[(]|[)]','') as year from movies where `_c0`!='movieId'").registerTempTable("gn_yr")
    sqlContext.sql("select * from gn_yr").withColumn("genre", explode(split($"genre", "[|]"))).groupBy("year","genre").count().show
  }

}
