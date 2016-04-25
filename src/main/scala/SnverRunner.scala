import scala.sys.process._

object SnverRunner {
  def runShell(inputFile:String, referenceFile:String): Stream[String] = {
    val snver_path = "/home/software/SNVer"
    val result = Seq("java", "-jar", snver_path + "/SNVerIndividual.jar", "-i", inputFile, "-r", referenceFile)
    result.lines
  }
}