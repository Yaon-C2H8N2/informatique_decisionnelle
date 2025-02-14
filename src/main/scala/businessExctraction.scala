import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object businessExctraction {
  def runPipeline(): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()
    val businessJsonFile = "data/yelp_academic_dataset_business.json"

    val businessJsonFileData = spark.read.json(businessJsonFile)

    businessJsonFileData.show(10, truncate = false)

    /**
     * {
     * "business_id": "f9NumwFMBDn751xgFiRbNA",
     * "name": "The Range At Lake Norman",
     * "address": "10913 Bailey Rd",
     * "city": "Cornelius",
     * "state": "NC",
     * "postal_code": "28031",
     * "latitude": 35.4627242,
     * "longitude": -80.8526119,
     * "stars": 3.5,
     * "review_count": 36,
     * "is_open": 1,
     * "attributes": {
     * "BusinessAcceptsCreditCards": "True",
     * "BikeParking": "True",
     * "GoodForKids": "False",
     * "BusinessParking": "{\"garage\": false, \"street\": false, \"validated\": false, \"lot\": true, \"valet\": false}",
     * "ByAppointmentOnly": "False",
     * "RestaurantsPriceRange2": "3"
     * },
     * "categories": "Active Life, Gun/Rifle Ranges, Guns & Ammo, Shopping",
     * "hours": {
     * "Monday": "10:0-18:0",
     * "Tuesday": "11:0-20:0",
     * "Wednesday": "10:0-18:0",
     * "Thursday": "11:0-20:0",
     * "Friday": "11:0-20:0",
     * "Saturday": "11:0-20:0",
     * "Sunday": "13:0-18:0"
     * }
     * }
     */

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
    geolocData.show(10, truncate = false)

    val businessFacts = businessData.sqlContext.sql(
      """
         SELECT
          business.business_id,
          v_geoloc.geolocation_id
         FROM business
         JOIN v_geoloc ON business.state = v_geoloc.state AND business.city = v_geoloc.city
      """
    )
    businessFacts.show(10, truncate = false)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    println("Writing " + businessData.count() + " rows to the database [business]")
    businessData.write
      .mode("append")
      .jdbc(jdbcUrl, "business", connectionProperties)

    println("Writing " + geolocData.count() + " rows to the database [geolocation]")
    geolocData.write
      .mode("append")
      .jdbc(jdbcUrl, "geolocation", connectionProperties)

    println("Writing " + businessFacts.count() + " rows to the database [business_facts]")
    businessFacts.write
      .mode("append")
      .jdbc(jdbcUrl, "business_facts", connectionProperties)
    println("Data written to the database")
  }
}
