import SavanaProjectPackage.ClinicalServiceMappingETL.clinicalServiceMapping
import org.apache.log4j._
import org.junit.Test
import org.junit.Assert.assertTrue

class ClinicalServiceMappingTest {

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Test where we check that "test1.csv" dataframe processed and mapped by the program is equal to the expected result
  @Test
  def test1(): Unit = {

    // obtain the processed and mapped "test1.csv" dataframe and the spark session instance
    val (resultDF,spark) = clinicalServiceMapping("test1.csv")

    // obtain the expected dataframe for the previous input
    val expectedTest1DF = spark.read.option("header", "true").csv("data/test1Expected.csv")

    // check that both are equals
    assertTrue(resultDF.except(expectedTest1DF).count==0)

    // close the spark session
    spark.close()
  }

  // Test where we check that "test2.csv" processed by the program is equal to the expected result
  @Test
  def test2(): Unit = {

    // obtain the processed and mapped "test1.csv" dataframe and the spark session instance
    val (resultDF,spark) = clinicalServiceMapping("test2.csv")

    // obtain the expected dataframe for the previous input
    val expectedTest2DF = spark.read.option("header", "true").csv("data/test2Expected.csv")

    // check that both are equals
    assertTrue(resultDF.except(expectedTest2DF).count==0)

    // close the spark session
    spark.close()
  }

  // Test where we check that "test3.csv" processed by the program is equal to the expected result
  @Test
  def test3(): Unit = {

    // obtain the processed and mapped "test1.csv" dataframe and the spark session instance
    val (resultDF,spark) = clinicalServiceMapping("test3.csv")

    // obtain the expected dataframe for the previous input
    val expectedTest3DF = spark.read.option("header", "true").csv("data/test3Expected.csv")

    // check that both are equals
    assertTrue(resultDF.except(expectedTest3DF).count==0)

    // close the spark session
    spark.close()
  }
}

