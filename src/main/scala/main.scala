import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()

    val businessesData = businessExctraction.runPipeline(spark)
    val checkinsData = checkinExtraction.runPipeline(spark)
    val usersData = userExtraction.runPipeline(spark)
    val tipsData = tipExtraction.runPipeline(spark, businessesData)
    val reviewsData = reviewExtraction.runPipeline(spark)
    val keywordsData = keywordExtraction.runPipeline(spark, reviewsData)

    businessFactBuilder.runPipeline(spark, businessesData, checkinsData, reviewsData, keywordsData)
    userFactBuilder.runPipeline(spark, usersData, reviewsData, tipsData)
  }
}