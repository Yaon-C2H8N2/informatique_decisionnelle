import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object businessExctraction {
  def runPipeline(): DataFrame = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()
    val businessJsonFile = "data/yelp_academic_dataset_business.json"

    val businessJsonFileData = spark.read.json(businessJsonFile)

    businessJsonFileData.createTempView("business")
    val businessData = businessJsonFileData.sqlContext.sql(
      """
        SELECT
         business_id, address, categories, is_open,
         latitude, longitude, name, postal_code, review_count, stars, hours.*
        FROM business
      """
    )
    businessData.createTempView("v_business")

    val geolocData = businessData.sqlContext.sql(
      """
         WITH geoloc AS (
          SELECT DISTINCT state, city
          FROM business
          WHERE city != ''
         )
         SELECT ROW_NUMBER() OVER (ORDER BY state, city) AS geolocation_id, state, city
         FROM geoloc
      """
    )
    geolocData.createTempView("v_geoloc")

    val businessFacts = businessData.sqlContext.sql(
      """
         SELECT
          business.business_id,
          v_geoloc.geolocation_id
         FROM business
         JOIN v_geoloc ON business.state = v_geoloc.state AND business.city = v_geoloc.city
      """
    )

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

//    println("Writing " + businessData.count() + " rows to the database [business]")
//    businessData.write
//      .mode("append")
//      .jdbc(jdbcUrl, "business", connectionProperties)
//
//    println("Writing " + geolocData.count() + " rows to the database [geolocation]")
//    geolocData.write
//      .mode("append")
//      .jdbc(jdbcUrl, "geolocation", connectionProperties)

    return businessFacts
  }
}
