
object main {
  def main(args: Array[String]): Unit = {
      businessExctraction.runPipeline()
      checkinExtraction.runPipeline()
  }
}