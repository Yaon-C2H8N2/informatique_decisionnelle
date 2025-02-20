import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, count, explode, expr, row_number}

object businessFactBuilder {
  def runPipeline(spark: SparkSession, business: DataFrame, checkins: DataFrame, reviews: DataFrame, keywords: DataFrame): Unit = {

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    val businessFact = business.as("business_facts")
      .join(keywords.as("business_with_keywords"), "business_id")
      .select("business_facts.business_id", "business_facts.geolocation_id", "business_with_keywords.year", "business_with_keywords.review_count", "business_with_keywords.stars", "business_with_keywords.keywords")

    val singleWordDF = businessFact
      .withColumn("keyword", explode(expr("split(concat_ws(' ', keywords), ' ')")))
      .select("business_id", "keyword", "year")

    val keywordCountDF = singleWordDF
      .groupBy("business_id", "year", "keyword")
      .agg(count("keyword").as("count"))

    val windowSpec = Window.partitionBy("business_id", "year").orderBy(col("count").desc)
    val rankedKeywordsDF = keywordCountDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") <= 15)
      .orderBy(col("business_id"), col("rank"))

    val aggregatedRankedKeywords = rankedKeywordsDF
      .groupBy("business_id", "year")
      .agg(collect_list("keyword").alias("keywords"))

    val resultDF = aggregatedRankedKeywords.as("agg_keywords")
      .join(businessFact.as("business_facts"), Seq("business_id", "year"))
      .select("business_facts.business_id", "business_facts.geolocation_id", "business_facts.year", "business_facts.review_count", "business_facts.stars", "agg_keywords.keywords")

    resultDF.show(10, truncate = false)

    println("Writing " + resultDF.count() + " rows to the database [business_facts]")
    resultDF.write
      .mode("append")
      .jdbc(jdbcUrl, "business_facts", connectionProperties)
  }
}
