import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import scala.util.matching.Regex.Match
import org.apache.spark.rdd._

object VcfToRehh {
  
  def chromosome_name(chromosome_name:String) = {
    val chromosome = chromosome_name.split("_")(1).replaceFirst ("^0*", "")
    
    if (chromosome.size > 2) {
      1
    } else {
      chromosome
    }
  }
  
  def gentype_to_haptype(gentype:String):String = {
    try {
      if (gentype.toInt == 0) {
        return "1"
      } 

      if (gentype.toInt == 1) {
        return "2"
      }
    } catch {
     case e: Exception => return "0"
   }
    
    "0"
  }
  
  def write_to_hapfile(vcfData:RDD[String], outputFile:String, population_index:Array[Int]) {
    val genotype_regex = """([^/]+)/([^:]+):""".r
    
    val hap_pw = new PrintWriter(new File(outputFile))
    val haplotypes = vcfData.filter(line => !line.contains("#"))
                .map(columns => columns.split("\t") )
                .map(selected_columns => (chromosome_name(selected_columns(0)), selected_columns(1), 
                                          selected_columns(3),
                                          selected_columns(4), selected_columns.splitAt(15)._2
                                         ))
                .filter{case(chromosome, position, ref_allele, alt_allele, sample_genotype_list) 
                          => ref_allele.size < 2 && 
                             alt_allele.split(",").intersect(Array("A", "C", "G", "T", "N", "*")).isEmpty}
                .zipWithIndex
                .map{case((chromosome, position, ref_allele, alt_allele, sample_genotype_list), snp_index) 
                     => sample_genotype_list
                          .map{ case(genotype) => genotype_regex.findFirstMatchIn(genotype).getOrElse(0d) }
                          .zipWithIndex
                          .map{ 
                            case(n:Double, sample_index) => (0, 0, sample_index, snp_index)
                            case (m:Match, sample_index) => (gentype_to_haptype(m.group(1)), 
                                                             gentype_to_haptype(m.group(2)), 
                                                             sample_index, snp_index)
                          }
                          .filter{case(_, _, sample_index, _) => population_index.contains(sample_index)}
                    }
                .fold(Array()) { (r, c) => r ++ c}
                .groupBy(_._3)
                .map{case(sample_index, haplotypes) => haplotypes.sortBy(_._4)}
                .map{case(haplotypes) => haplotypes.map {
                                          case(hap1, hap2, sample_index, snp_index) => 
                                              (hap1, hap2)
                                         }
                    }
                .map{case(haplotype_strs) => (
                    haplotype_strs.fold("") { case (r,(hap1:String, hap2:String)) => r + hap1 + " " },
                    haplotype_strs.fold("") { case (r,(hap1:String, hap2:String)) => r + hap2 + " " }
                    )
                   }
                .foldLeft(Array[String]()) { case(r, (hap1:String, hap2:String)) => r :+ hap1 :+ hap2 }
                .zipWithIndex
                .map{case(haplotype_str, index) => index + " " + haplotype_str + "\n"}
                .foreach(l => hap_pw.write(l))
    hap_pw.close
  }
  
  def main(args:Array[String]) {
    val vcfFile = "/sandbox/SNP_INDEL_Pf3D7_12_v3.combined.filtered.vcf" 
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val vcfData = sc.textFile(vcfFile, 2)
       
    /*val pw = new PrintWriter(new File("/sandbox/snp_map.map"))
    val snp_map = vcfData.filter(line => !line.contains("#"))
                .map(columns => columns.split("\t") )
                .map(selected_columns => (chromosome_name(selected_columns(0)), selected_columns(1), 
                                          selected_columns(3),
                                          selected_columns(4)))
                .filter{case(chromosome, position, ref_allele, alt_allele) 
                          => ref_allele.size < 2 && 
                             alt_allele.split(",").intersect(Array("A", "C", "G", "T", "N", "*")).isEmpty}
                .zipWithIndex()
                .map {case ((chromosome, position, ref_allele, alt_allele), index) => 
                        "F" + chromosome + "_" + index + " " + chromosome + " " + position + " 0 0\n" }
                .collect()
                .foreach(l => pw.write(l))
    pw.close*/
    
    /*val sample_list = vcfData.filter(line => line.contains("#CHROM")).first()
                            .split("\t")
                            .zipWithIndex
    val pop1 = sample_list.splitAt(1000)._1.map{case(sample, index) => index}
    val pop2 = sample_list.splitAt(1000)._2.map{case(sample, index) => index}*/
    
    val pop1 = sc.textFile("/sandbox/kintampo_index.out", 2)
                 .map(line => line.toInt).toArray
    val pop2 = sc.textFile("/sandbox/navrongo_index.out", 2)
                 .map(line => line.toInt).toArray
    
    write_to_hapfile(vcfData, "/sandbox/hap1.out", pop1)
    write_to_hapfile(vcfData, "/sandbox/hap2.out", pop2)
                
    println("\n\n\n")
    println("Hello World")
    println("\n\n\n")
  }
}