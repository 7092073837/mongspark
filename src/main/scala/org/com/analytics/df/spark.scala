package org.com.analytics.df

import org.apache.spark.sql.SparkSession

object spark {

  val spark_session = SparkSession
    .builder()
    .appName("spark_application_name")
    .master(("local"))
    .getOrCreate()
}
