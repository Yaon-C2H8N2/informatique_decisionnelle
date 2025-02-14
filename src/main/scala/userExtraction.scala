import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.monotonically_increasing_id

object userExtraction {
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

    val user = spark.read
      .jdbc(jdbcUrlSource, "yelp.user", connectionPropertiesSource)
      .select("user_id", "name", "yelping_since")

    println("Writing " + user.count() + " rows to the database [users]")
    user.write
      .mode("append")
      .jdbc(jdbcUrlDestination, "users", connectionPropertiesDestination)
    println("Data written to the database")
  }
}
