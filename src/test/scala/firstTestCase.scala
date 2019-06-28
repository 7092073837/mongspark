
import com.sun.javafx.binding.StringConstant
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types
import org.com.analytics.rdd.release_year.{Movies, movies, sc}
import org.scalatest.FunSuite

class firstTestCase extends FunSuite {

  test("example testcase"){
    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    import sqlContext.implicits._
    val moviesRDD = sc.textFile(movies)
    val moviesHeader = moviesRDD.first()
    val moviesWOHeaderRDD = moviesRDD.filter(row => row != moviesHeader)
    val moviesDF = moviesWOHeaderRDD.map(_.split(",")).map(p=>Movies(p(0).toInt,p(1),p(2))).toDF
    moviesDF.registerTempTable("movies")
    val result = sqlContext.sql("select movieId,title,genres,REGEXP_REPLACE(substr(title,length(title)-6,7),'[(]|[)]','') as year from movies limit 10").show


    val s1 = "f1"
    assert("f1" === s1)
    //assert(result.c ==="as")
    //assert(types.StructField("f1", StringType, false) == s1.getStructField)
  }

}
