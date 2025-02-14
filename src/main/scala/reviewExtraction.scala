import org.apache.spark.sql.SparkSession

object reviewExtraction {
  def runPipeline(): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()

    val jdbcUrlSource = "jdbc:postgresql://stendhal:5432/tpid2020"
    val connectionPropertiesSource = new java.util.Properties()
    connectionPropertiesSource.setProperty("user", "tpid")
    connectionPropertiesSource.setProperty("password", "tpid")
    connectionPropertiesSource.setProperty("driver", "org.postgresql.Driver")

    val jdbcUrlDestination = "jdbc:postgresql://localhost:5432/hop"
    val connectionPropertiesDestination = new java.util.Properties()
    connectionPropertiesDestination.setProperty("user", "hop")
    connectionPropertiesDestination.setProperty("password", "hop")
    connectionPropertiesDestination.setProperty("driver", "org.postgresql.Driver")

    val partitions = spark.read
      .jdbc(jdbcUrlSource, "yelp.review", connectionPropertiesSource)
      .select("spark_partition")
      .distinct()
      .count()

    for (i <- 0 to partitions.toInt) {
      val reviews = spark.read
        .jdbc(jdbcUrlSource, "yelp.review", connectionPropertiesSource)
        .select("review_id", "user_id", "business_id", "date", "text", "stars", "cool", "funny", "useful")
        .where("spark_partition = " + i)
      reviews.show(10, truncate = false)
      println("Writing " + reviews.count() + " rows to the database [reviews]")
      reviews.write
        .mode("append")
        .jdbc(jdbcUrlDestination, "reviews", connectionPropertiesDestination)
    }
    println("Data written to the database")
  }
}
