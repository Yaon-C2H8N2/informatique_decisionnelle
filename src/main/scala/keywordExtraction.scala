import org.apache.spark.sql.SparkSession

object keywordExtraction {
  def runPipeline(): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()
    val keywordsJsonFile = "data/keywords_output.json"

    val keywordsJsonFileData = spark.read.json(keywordsJsonFile)
    keywordsJsonFileData.createTempView("keywords")

    spark.sql("SELECT * FROM keywords").show(10, truncate = false)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    val reviewsWithKeywords = spark.read
      .jdbc(jdbcUrl, "reviews", connectionProperties)
      .join(keywordsJsonFileData, "review_id")
    reviewsWithKeywords.show(10, truncate = false)
    reviewsWithKeywords.createTempView("reviewsWithKeywords")

    val businessWithKeywords = reviewsWithKeywords.sqlContext.sql(
      """
         SELECT business_id, flatten(collect_list(keywords)) as keywords
         FROM reviewsWithKeywords
         GROUP BY business_id
      """
    )
    businessWithKeywords.show(10, truncate = false)
  }
}
