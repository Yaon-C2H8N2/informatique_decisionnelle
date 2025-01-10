import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object checkinExtraction {
  def runPipeline(): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local").getOrCreate()
    val checkinJsonFile = "data/yelp_academic_dataset_checkin.json"

    val checkinJsonFileData = spark.read.json(checkinJsonFile)
    checkinJsonFileData.createTempView("checkin")

    checkinJsonFileData
      .withColumn("processed_date", split(col("date"), ","))
      .withColumn("processed_date", explode(col("processed_date")))
      .withColumn("processed_date", ltrim(col("processed_date")))
      .withColumn("processed_date", date_format(col("processed_date"), "yyyy-MM-dd"))
      .withColumn("processed_date", to_timestamp(col("processed_date"), "yyyy-MM-dd"))
      .createTempView("v_checkin")

    val checkinDF = checkinJsonFileData.sqlContext.sql("SELECT business_id, processed_date FROM v_checkin")
    checkinDF.show(10, truncate = false)

    val timeDF = checkinJsonFileData.sqlContext.sql(
      """
        SELECT DISTINCT processed_date AS datetime
        FROM v_checkin
      """
    )
    timeDF
      .withColumn("time_id", monotonically_increasing_id())
      .createTempView("v_time")

    val timeDimensionDF = timeDF.sqlContext.sql("SELECT * FROM v_time")
    timeDimensionDF.show(10, truncate = false)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    println("Writing " + timeDimensionDF.count() + " rows to the database [time]")
    timeDimensionDF.write
      .mode("append")
      .jdbc(jdbcUrl, "time", connectionProperties)
    println("Data written to the database")
  }
}
