import org.apache.spark.sql.SparkSession

object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Main ETL Pipeline").master("local[*]").getOrCreate()

    val businessesData = businessExctraction.runPipeline(spark)
    val checkinsData = checkinExtraction.runPipeline(spark)
    userExtraction.runPipeline(spark)
    tipExtraction.runPipeline(spark)
    val reviewsData = reviewExtraction.runPipeline(spark)
    val keywordsData = keywordExtraction.runPipeline(spark, reviewsData)

    businessFactBuilder.runPipeline(spark, businessesData, checkinsData, reviewsData, keywordsData)
  }
}