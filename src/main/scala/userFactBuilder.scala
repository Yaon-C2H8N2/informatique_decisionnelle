import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object userFactBuilder {
  def runPipeline(spark: SparkSession, users: DataFrame, reviews: DataFrame, tips: DataFrame): Unit = {

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    reviews.createTempView("reviews")
    val reviewAgg = reviews.sqlContext.sql(
      """
        select
          user_id,
          date_part('year', date) as year,
          count(review_id) as review_count,
          round(avg(stars), 2) as average_stars,
          count(distinct business_id) as business_count,
          sum(cool + funny + useful) as reactions_count
        from reviews
        group by user_id, date_part('year', date)
      """
    )

    tips.createTempView("tips")
    val tipAgg = tips.sqlContext.sql(
      """
        select
          user_id,
          date_part('year', date) as year,
          sum(compliment_count) as compliment_count,
          count(tip_id) as tip_count
        from tips
        group by user_id, date_part('year', date)
      """
    )

    val resultDF = users.as("users")
      .join(reviewAgg.as("reviewsAgg"), users("user_id") === reviewAgg("user_id"), "left")
      .join(tipAgg.as("tipsAgg"), users("user_id") === tipAgg("user_id") && reviewAgg("year") === tipAgg("year"), "left")
      .select(
        users("user_id"),
        coalesce(reviewAgg("year"), tipAgg("year")) as "year",
        coalesce(reviewAgg("review_count"), lit(0)) as "review_count",
        coalesce(reviewAgg("average_stars"), lit(0)) as "average_stars",
        coalesce(reviewAgg("business_count"), lit(0)) as "business_count",
        coalesce(reviewAgg("reactions_count"), lit(0)) as "reactions_count",
        coalesce(tipAgg("compliment_count"), lit(0)) as "compliment_count",
        coalesce(tipAgg("tip_count"), lit(0)) as "tip_count"
      )

    println("Writing " + resultDF.count() + " rows to the database [users_facts]")
    resultDF.write
      .mode("append")
      .jdbc(jdbcUrl, "users_facts", connectionProperties)
  }
}
