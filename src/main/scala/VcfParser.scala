import scala.sys.process._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.matching.Regex.Match
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.linalg.Vectors
//import org.apache.log4j.Logger

object VcfParser {
    def main(args:Array[String]) {
      val sampleVcfFile = "/sandbox/plasmodium_data/PA0026-C.vcf" 
      val populationVcfFile = "/sandbox/plasmodium_data/PA0026-C.vcf"
      //val log = Logger.getLogger(getClass.getName)
      
      val conf = new SparkConf().setAppName("Simple Application")
      val sc = new SparkContext(conf)
      val vcfData = sc.textFile(sampleVcfFile, 2).cache()
      val populationVcfData = sc.textFile(populationVcfFile, 2).cache()
      
      // create 10 equally-sized MAF inverals ([0.0-0.05], [0.05-0.1] ... [0.45-0.5])
      
      def mean(xs: Iterable[Double]) = xs.sum / xs.size
      
      def divide(xs: Iterable[Double]) = xs.foldLeft(1d) ((r,c) => c * 1/r)
      
      def mergeZeroAndOneBin(x:Int) = {
        if (x == 0)
          1
        else
          x
      }
      
      /*val Hsf = vcfData.filter(line => !line.contains("#"))
                    .map(description => description.split("\t")(9))
                    .map(countArray => countArray.split(":"))
                    .map(tuple3 => Tuple4(tuple3(2).toDouble +tuple3(3).toDouble + 
                                          tuple3(4).toDouble + tuple3(5).toDouble, 
                                          tuple3(2).toDouble + tuple3(3).toDouble, 
                                          tuple3(4).toDouble + tuple3(5).toDouble,
                                          math.min((tuple3(2).toDouble + tuple3(3).toDouble) / (tuple3(2).toDouble +tuple3(3).toDouble + 
                                          tuple3(4).toDouble + tuple3(5).toDouble), 
                                              (tuple3(4).toFloat + tuple3(5).toDouble) / (tuple3(2).toDouble +tuple3(3).toDouble + 
                                          tuple3(4).toDouble + tuple3(5).toDouble)
                                          )    
                                         ))
                    .groupBy(mafInterval => mergeZeroAndOneBin((mafInterval._4 / 0.05).toInt))
                    .map{case(mafInterval, array) => (mafInterval, 
                                                       array.map(
                                                         Hsx => 1 - ( (Hsx._2/Hsx._1) * (Hsx._2/Hsx._1)  
                                                         + (Hsx._3/Hsx._1) * (Hsx._3/Hsx._1) ) 
                                                       )
                                                      )}
                    .map{case(mafInterval, avg) => (mafInterval, ("Hsf", mean(avg)))} //Hs,f value at bin[0.0-0.05]...bin[0.45-0.50]
                    .cache()*/
      
      val pattern = """AF=([^;]+)""".r
      
      val Hsf = vcfData.filter(line => !line.contains("#"))
                    .map(description => description.split("\t")(7))
                    .map(alternateAlleleFrequency => pattern.findFirstMatchIn(alternateAlleleFrequency).getOrElse(0d))
                    .map{ 
                      case(n:Double) => n
                      case (m:Match) => m.group(1).split(",").map(freq => freq.toDouble).sum
                     }
                    .groupBy(mafInterval => mergeZeroAndOneBin((mafInterval / 0.05).toInt))
                    .map{case(mafInterval, array) => (mafInterval,
                                                      array.map(
                                                        Hsx => 1 - ((Hsx*Hsx) + (1-Hsx)*(1-Hsx))
                                                      )
                                                     )}
                    .map{case(mafInterval, avg) => (mafInterval, ("Hpf", mean(avg)))} //Hp,f value at bin[0.0=0.05]...bin[0.45-0.50]
                    .cache()
      
      val Hpf = populationVcfData.filter(line => !line.contains("#"))
                    .map(description => description.split("\t")(7))
                    .map(alternateAlleleFrequency => pattern.findFirstMatchIn(alternateAlleleFrequency).getOrElse(0d))
                    .map{ 
                      case(n:Double) => n
                      case (m:Match) => m.group(1).split(",").map(freq => freq.toDouble).sum
                     }
                    .map { freq => if (freq > 0.5) 1-freq else freq }
                    .groupBy(mafInterval => mergeZeroAndOneBin((mafInterval / 0.05).toInt))
                    .map{case(mafInterval, array) => (mafInterval,
                                                      array.map(
                                                        Hpx => 1 - ((Hpx*Hpx) + (1-Hpx)*(1-Hpx))
                                                      )
                                                     )}
                    .map{case(mafInterval, avg) => (mafInterval, ("Hpf", mean(avg)))} //Hp,f value at bin[0.0=0.05]...bin[0.45-0.50]
                    .cache()
      Hpf.collect().foreach(p => println(p))
      val Hsf_Hpf = (Hsf ++ Hpf).groupByKey().map{case(mafInterval, coordinates) => coordinates }
                                             .map(coordinates => 
                                                  coordinates.toList.sortBy(_._1).map(coordinate => coordinate._2)
                                             )
                                            
      //log.warn("VCF Count:" + Hpf)
      println("Hello!\n\n\n")
      println("VCF Count:" + Hsf_Hpf.count)
      
      //map (Hsf,Hpf)
      val labeledPoints = Hsf_Hpf.map(p => (p.head, Vectors dense(p.drop(1).toArray)) )
                                  .filter(_._2.size > 0) //filter out vectors that are empty
                                  .map{ case(label, vector) => LabeledPoint(label, vector)}
                                  .cache()
      
      labeledPoints.collect().foreach(p => println(p))
      
      //val algorithm = new LinearRegressionWithSGD()
      val numIterations = 100
      val steps = 0.2
      //val model = LinearRegressionWithSGD.train(labeledPoints, numIterations) //algorithm run labeledPoints
      
      /*println("Hello!\n\n\n\n!")
      println("Intercept:")
      println(model.intercept)
      println(model.weights)
      
      val coefficient = model.weights.toArray.head
      val F_ws = 1 - coefficient
      println("F_ws:" + F_ws)*/
    }
}