import org.com.analytics.rdd.release_year
import org.junit.Test
import org.junit.Assert._

class moviesReleaseYear extends TestCase {

  var releaseYear: releaseYear = _
  @test
  override def setUp {
    releaseYear = new release_year.Movies()
  }

  @After
  def tearDown(): Unit = {
    spark.stop
    spark = null
  }

  def testReleaseYear {
    release_year
    /*pizza.addTopping(Topping("green olives"))
    assertEquals(pizza.getToppings.size, 1)*/
  }

}