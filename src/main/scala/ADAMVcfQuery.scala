import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.formats.avro.GenotypeAllele
import org.apache.avro.generic._
import org.bdgenomics.utils.cli._
import org.apache.spark.rdd.RDD
import java.io._

import org.rosuda.JRI.Rengine

object ADAMVcfQuery {
  
  def genotype_to_haptype(allele:GenotypeAllele):String = {
    allele match { case GenotypeAllele.Ref => "1" 
                   case GenotypeAllele.Alt => "0" 
                   case GenotypeAllele.OtherAlt => "0"
                   case GenotypeAllele.NoCall => "0"
                 }
  }
  
  def alleles_to_haptypes(alleles:List[GenotypeAllele]):Tuple2[String, String] = {
    (genotype_to_haptype(alleles.get(0)), genotype_to_haptype(alleles.get(1)))
  }
  
  def write_snp_info(chromosome:String, genotypes:RDD[Genotype], outputFile:String, pop1:Iterable[String], 
                     pop2:Iterable[String]) {
    val pw = new PrintWriter(new File(outputFile))
    println(genotypes.count())
    //genotypes.filter{g => g.variant != null && g.alleles != null}
             //.map{g => g.variant.start.toInt}
             //.sortBy{position => position}
             //.collect
             //.distinct
             //.zipWithIndex
             //.map {case (position, index) => 
             //           "F" + chromosome + "_" + index + " " + chromosome + " " + position + " 0 0\n" }
             //.foreach(l => pw.write(l))
    pw.close
  }
  
  def write_to_hapfile(genotypes:RDD[Genotype], outputFile:String, pop:Iterable[String]) {
    val hap_pw = new PrintWriter(new File(outputFile))
    
    val positions = genotypes.filter{g => g.variant != null && g.alleles != null}
             .map{g => g.variant.start.toInt}
             .sortBy{position => position}
             .collect
             .distinct
    
    genotypes.filter{g => pop.contains(g.sampleId) && g.variant != null && g.alleles != null}
             .map{g => (g.sampleId, g.variant.start, alleles_to_haptypes(g.alleles)) }
             .groupBy(_._1) // group by samples 
             .map{case(_, genotype_array) => 
                          genotype_array.toList.groupBy(_._2)
                                               .map{case(_, groupByPosition) => groupByPosition.head}
                                               .toList // hack to get distinct SNP positions(!); figure out why the SNPs are not biallelic??
                                               .sortBy(_._2)} // order by position 
             .map{genotype_array => ( 
                  genotype_array.fold("") {case (r, (_, _, (hap1:String, _))) => r + hap1 + " "},
                  genotype_array.fold("") {case (r, (_, _, (_, hap2:String))) => r + hap2 + " "}
                  )
              } // an array of tuples of haplotypes 
             .collect 
             .foldLeft(Array[String]()) { case(r, (hap1:String, hap2:String)) => r :+ hap1 :+ hap2 }
             .zipWithIndex
             .map{case(haplotype_str, index) => index + " " + haplotype_str + "\n"} // convert to rehh haplotype file
             .foreach(l => hap_pw.write(l))
    hap_pw.close
  }
  
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("adam: ADAM2Vcf")
    val sc = new SparkContext(conf)
    
    val pop1 = sc.textFile("/sandbox/kintampo_samples.txt", 2).toArray
    val pop2 = sc.textFile("/sandbox/navrongo_samples.txt", 2).toArray
    
    val genotypes:RDD[Genotype] = sc.loadParquetGenotypes("/sandbox/adam_chr_12/")
    write_snp_info("10", genotypes, "/sandbox/snp_map_adam2.info", pop1, pop2)
    //write_to_hapfile(genotypes, "/sandbox/hap1_adam.out", pop1)
    //write_to_hapfile(genotypes, "/sandbox/hap2_adam.out", pop2)
    
    /*val r = new Rengine (Array("--vanilla"), false, null)
    val result1 = r.eval("capture.output(library(rehh))")
    result1.asStringArray().foreach(line => println(line))
    
    val result2 = r.eval("capture.output(source('/sandbox/ies2rsb.r'))")
    result2.asStringArray().foreach(line => println(line))
    
    val result3 = r.eval("capture.output(data<-data2haplohh(hap_file='/sandbox/hap1.out','/sandbox/snp_map.map',chr.name=12))")
    result3.asStringArray().foreach(line => println(line))

    val result4 = r.eval("capture.output(data2<-data2haplohh(hap_file='/sandbox/hap2.out','/sandbox/snp_map.map',chr.name=12))")    
    result4.asStringArray().foreach(line => println(line))
    
    println("scan_hh(data)")
    r.eval("res1<-scan_hh(data)")
    println("scan_hh(data2)")
    r.eval("res2<-scan_hh(data2)")
    
    r.eval("wg.res.pop1<-res1")
    r.eval("wg.res.pop2<-res2")
    
    val result5 = r.eval("capture.output(wgscan.rsb<-ies2rsb2(wg.res.pop1, wg.res.pop2))")
    result5.asStringArray().foreach(line => println(line))
    
    r.eval("jpeg(file=\"/sandbox/plot_test.jpg\")")
    val result6 = r.eval("capture.output(rsbplot(wgscan.rsb$res.rsb))")
    result6.asStringArray().foreach(line => println(line))
    r.eval("dev.off()")

    r.end*/    
  }
}
