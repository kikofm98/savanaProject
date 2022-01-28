package SavanaProjectPackage

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.log4j._

object ClinicalServiceMappingETL {

  /**
   * updates the selected date of the dataframe changing the type and format
   * @param records - dataframe which is going to modify its date type and format
   * @param column - column that is going to be formated
   * @param newColumn - the new name for the previous column
   * @return the resulting dataframe
   */
  def updateDateFormat(records: DataFrame, column: String, newColumn: String) : DataFrame = {

    // Changes the date format (recognize the following formats: "dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd", "MM-yyyy", "dd MM yyyy")
    records.withColumn(column, coalesce(to_date(col(column), "dd-MM-yyyy"), to_date(col(column), "yyyy-MM-dd"),
      to_date(col(column), "MM-yyyy"),to_date(col(column), "dd/MM/yyyy"),to_date(col(column), "dd MM yyyy")))
      .withColumnRenamed(column,newColumn)

  }

  /**
   * main method which process the csv file and returns de resulting dataframe
   * @param fileCSV - csv file to be processed
   * @return requested dataframe and the spark session instance
   */
  def clinicalServiceMapping(fileCSV: String) : (DataFrame,SparkSession ) = {

    // Sets the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Uses new SparkSession interface in Spark 3.0
    val spark = SparkSession.builder.appName("SavanaProject").master("local[*]").getOrCreate()

    // Converts the csv file to a DataFrame
    val records = spark.read.option("header", "true").csv("data/"+fileCSV)

    // Imports csv files to map the previous dataframe changing the data of the gender and service column
    val serviceLines = spark.sparkContext.textFile("data/OriginalService-SavanaService.csv")
    val genderLines = spark.sparkContext.textFile("data/OriginalGender-SavanaGender.csv")

    // Splits by comas the previous csv and create the RDDs
    val serviceRDD = serviceLines.map(x => (x.split(",")(0),(x.split(",")(1))))
    val genderRDD = genderLines.map(x => (x.split(",")(0),(x.split(",")(1))))

    // Converts RDD into a map
    val serviceMap = serviceRDD.collect().toMap
    val serviceColumnMap = typedLit(serviceMap)

    // Converts the map into a columnMap
    val genderMap = genderRDD.collect().toMap
    val genderColumnMap = typedLit(genderMap)

    // Maps the "original_service" and "original_gender" column obtaining their new values, rename the columns ("service" and "gender") and cast into the new types.
    val servicesUpdated = records.withColumn("original_service", serviceColumnMap(col("original_service")))
      .withColumnRenamed("original_service","service")
    val genderUpdated = servicesUpdated.withColumn("original_gender",
      when(genderColumnMap(col("original_gender")).isNotNull, genderColumnMap(col("original_gender")).cast("int"))
        .otherwise(0)).withColumnRenamed("original_gender","gender")

    // Changes the "original_document_date" and "original_birthday" columns format and rename the columns ("document" and "birthday").
    // Calls the updateDateFormat method.
    val recordsDocumentUpdated= updateDateFormat(genderUpdated,"original_document_date","document_date")
    val recordsBirthdayUpdated= updateDateFormat(recordsDocumentUpdated,"original_birthdate","birthdate")

    // Casts "original_patient_id" and "original_document_id" into a long type and rename columns (patient_id, document_id)
    val finalRecords = recordsBirthdayUpdated.withColumn("original_patient_id",col("original_patient_id").cast("long"))
      .withColumnRenamed("original_patient_id","patient_id")
      .withColumn("original_document_id",col("original_document_id").cast("long"))
      .withColumnRenamed("original_document_id","document_id")

    // Returns resulting dataframe and the spark session
    return (finalRecords,spark)

  }


  def main(args: Array[String]) {

    // Calls clinicalServiceMapping method
    val (result,sparkSession) = clinicalServiceMapping("test3.csv")

    // Writes a new csv file with the resulting dataframe data
    result.coalesce(1).write.option("header","true").mode("overwrite").csv("data/recordsFinal.csv")

    // Close the spark session
    sparkSession.close()

  }
}