import scala.io.Source
import java.io._

object IdentifierExtractor {
  def main(args:Array[String]) {
    val inputFile = "/sandbox/kintampo.csv"
    val lines = Source.fromFile(inputFile).getLines.toList
    
    val kintampo_list = lines.filter(l => !l.startsWith("Population"))
                        .map(ers => ers.split(",")(2))
    
    val metadataFile = "/sandbox/pf3k_release_4_metadata.txt"
    val metadataLines = Source.fromFile(metadataFile).getLines.toList
    
    val kintampo_pw = new PrintWriter(new File("/sandbox/kintampo_index.out"))
    val kintampoSamples = metadataLines.filter(l => !l.startsWith("sample"))
                 .map(columns => columns.split("\t"))
                 .zipWithIndex
                 .map{ case(selected_columns, index) => (selected_columns(0), selected_columns(1), index) }
                 .filter{ case(_, ers, _) => kintampo_list.contains(ers) }
                 .foreach{ case(_, _, index) => kintampo_pw.println(index) }
    kintampo_pw.close
    
    val navrongoFile = "/sandbox/navrongo.csv"
    val navrongoLines = Source.fromFile(navrongoFile).getLines.toList
    
    val navrongo_list = navrongoLines.map(ers => ers.split(",")(2))
    println("\n\n\n")
    
    val navrongo_pw = new PrintWriter(new File("/sandbox/navrongo_index.out"))
    val navrongoSamples = metadataLines.filter(l => !l.startsWith("sample"))
                 .map(columns => columns.split("\t"))
                 .zipWithIndex
                 .map{ case(selected_columns, index) => (selected_columns(0), selected_columns(1), index) }
                 .filter{ case(_, ers, _) => navrongo_list.contains(ers) }
                 .foreach{ case(_, _, index) => navrongo_pw.println(index) }
    navrongo_pw.close             
  }
}