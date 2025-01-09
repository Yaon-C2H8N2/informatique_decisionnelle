import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object main {
  Logger.getLogger("org").setLevel(org.apache.log4j.Level.OFF)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local").getOrCreate()
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
         business_id, address, city, categories, is_open,
         latitude, longitude, name, postal_code, review_count, stars, state, hours.*
        FROM business
      """
    )
    businessData.createTempView("v_business")

    val attributesData = businessJsonFileData.sqlContext.sql(
      """
        SELECT attributes.*
        FROM business
      """
    )
    val attributesNames = attributesData.columns

    val attributesSchema = StructType(Seq(
      StructField("attribute_id", IntegerType, nullable = false),
      StructField("attribute_name", StringType, nullable = false)
    ))

    val attributesRows = attributesNames.zipWithIndex.map { case (name, id) =>
      Row(id, name)
    }

    val attributesDF = spark.createDataFrame(
      spark.sparkContext.parallelize(attributesRows),
      attributesSchema
    )
    attributesDF.createTempView("v_attributes")
    attributesDF.show(10, truncate = false)

    val attributesDataUnion = attributesNames.map { name =>
      businessJsonFileData.sqlContext.sql(
        s"""
          SELECT business_id, attribute_id, business.attributes['$name'] as attribute_value
          FROM business
          JOIN v_attributes ON '$name' = v_attributes.attribute_name
          WHERE attributes['$name'] IS NOT NULL
        """
      )
    }
    val attributesDataUnionDF = attributesDataUnion.reduce(_ union _)
    attributesDataUnionDF.show(10, truncate = false)

    val jdbcUrl = "jdbc:postgresql://localhost:5432/hop"
    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", "hop")
    connectionProperties.setProperty("password", "hop")
    connectionProperties.setProperty("driver", "org.postgresql.Driver")

    println("Writing " + businessData.count() + " rows to the database [business]")
    businessData.write
      .mode("append")
      .jdbc(jdbcUrl, "business", connectionProperties)
    println("Data written to the database")

    println("Writing " + attributesDF.count() + " rows to the database [attributes]")
    attributesDF.write
      .mode("append")
      .jdbc(jdbcUrl, "attributes", connectionProperties)
    println("Data written to the database")

    println("Writing " + attributesDataUnionDF.count() + " rows to the database [attributes]")
    attributesDataUnionDF.write
      .mode("append")
      .jdbc(jdbcUrl, "business_facts", connectionProperties)
    println("Data written to the database")
  }
}