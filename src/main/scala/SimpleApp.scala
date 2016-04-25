import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args:Array[String]) {
    val logFile = "/home/software/spark-1.5.2/README.md" // Should be some file on your system
    val inputFile = args(0)
    val referenceFile = args(1)
    
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    println(SnverRunner.runShell(inputFile, referenceFile))
  }
}