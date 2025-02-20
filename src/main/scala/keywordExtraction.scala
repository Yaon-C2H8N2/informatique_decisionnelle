import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, count, explode, expr, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object keywordExtraction {
  def runPipeline(spark: SparkSession, reviews: DataFrame): DataFrame = {
    val keywordsJsonFile = "data/keywords_output.json"

    val keywordsJsonFileData = spark.read.json(keywordsJsonFile)
    keywordsJsonFileData.createTempView("keywords")

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    val reviewsWithKeywords = reviews.as("reviews")
      .join(keywordsJsonFileData, "review_id", "left")
      .withColumn("keywords", expr("coalesce(keywords, array())"))
    reviewsWithKeywords.createTempView("reviewsWithKeywords")

    val businessWithKeywords = reviewsWithKeywords.sqlContext.sql(
      """
         WITH distinct_years AS (
            SELECT date_part('year', date) as year
            FROM reviewsWithKeywords
            GROUP BY date_part('year', date)
         )
         SELECT business_id, array_distinct(flatten(collect_list(keywords))) as keywords, count(review_id) as review_count, avg(stars) as stars, distinct_years.year
         FROM reviewsWithKeywords
         JOIN distinct_years ON date_part('year', date) = distinct_years.year
         GROUP BY business_id, distinct_years.year
      """
    )

    return businessWithKeywords
  }
}
