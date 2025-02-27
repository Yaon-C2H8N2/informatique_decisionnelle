import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id

object tipExtraction {
  def runPipeline(spark: SparkSession, business: DataFrame): DataFrame = {
    val tipCsvFile = "data/yelp_academic_dataset_tip.csv"

    val tipCsvFileData = spark.read.option("header", "true").csv(tipCsvFile)

    val tipData = tipCsvFileData.withColumn("tip_id", monotonically_increasing_id())
    val tipDataWithFormattedReviewCount = tipData.withColumn("compliment_count", tipData("compliment_count").cast("int"))
    val tipDataWithFormattedDate = tipDataWithFormattedReviewCount.withColumn("date", tipDataWithFormattedReviewCount("date").cast("timestamp"))

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    business.select("business_id").createTempView("v_business_ids")
    val resTipData = tipDataWithFormattedDate
      .select("tip_id", "user_id", "business_id", "compliment_count", "date", "text")
      .join(spark.sql("SELECT business_id FROM v_business_ids"), "business_id")
      .where("user_id != '' AND user_id is not null" )
    resTipData.show(10, truncate = false)

    println("Writing " + tipData.count() + " rows to the database [tips]")
    resTipData.write
      .mode("append")
      .jdbc(jdbcUrl, "tips", connectionProperties)
    println("Data written to the database")

    return resTipData
  }
}
