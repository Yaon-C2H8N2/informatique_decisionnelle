import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object checkinExtraction {
  def runPipeline(spark: SparkSession): DataFrame = {
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

    val checkinDF = checkinJsonFileData.sqlContext.sql("SELECT business_id, processed_date as date, count(processed_date) as checkins_count FROM v_checkin GROUP BY business_id, processed_date")
    checkinDF.show(10, truncate = false)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    println("Writing " + checkinDF.count() + " rows to the database [checkin]")
    checkinDF.write
      .mode("append")
      .jdbc(jdbcUrl, "checkins", connectionProperties)
    println("Data written to the database")

    return checkinDF
  }
}
