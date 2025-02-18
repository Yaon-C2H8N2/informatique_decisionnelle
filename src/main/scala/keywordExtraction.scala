import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, count, explode, expr, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object keywordExtraction {
  def runPipeline(businessFacts: DataFrame): Unit = {
    /**
     * with
     * single_word as (
     * select business_id, unnest(string_to_array(unnest(keywords), ' ')) as keyword
     * from business_facts_test_omg
     * ),
     * keyword_count as (
     * select business_id, keyword, count(*) as count
     * from single_word
     * group by business_id, keyword
     * order by count(*) desc
     * ),
     * ranked_keywords AS (
     * SELECT business_id, keyword, count,
     * ROW_NUMBER() OVER (PARTITION BY business_id ORDER BY count DESC) AS rank
     * FROM keyword_count
     * )
     * select business_id, array_agg(keyword) as keywords
     * from ranked_keywords
     * group by business_id;
     */

    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()
    val keywordsJsonFile = "data/keywords_output.json"

    val keywordsJsonFileData = spark.read.json(keywordsJsonFile)
    keywordsJsonFileData.createTempView("keywords")

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    val reviewsWithKeywords = spark.read
      .jdbc(jdbcUrl, "reviews", connectionProperties)
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

    val businessFact = businessFacts.as("business_facts")
      .join(businessWithKeywords.as("business_with_keywords"), "business_id")
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
