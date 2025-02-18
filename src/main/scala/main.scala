
object main {
  def main(args: Array[String]): Unit = {
    val businessFacts = businessExctraction.runPipeline()
//    checkinExtraction.runPipeline()
//    userExtraction.runPipeline()
//    tipExtraction.runPipeline()
//    reviewExtraction.runPipeline()
    keywordExtraction.runPipeline(businessFacts)
  }
}