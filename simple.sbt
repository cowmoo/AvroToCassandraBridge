name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "org.bdgenomics.adam" %% "adam-cli" % "0.18.2"
libraryDependencies += "org.bdgenomics.adam" %% "adam-apis" % "0.18.2"
libraryDependencies += "org.bdgenomics.adam" %% "adam-core" % "0.18.2"
libraryDependencies += "org.bdgenomics.adam" %% "adam-parent" % "0.18.2"
libraryDependencies += "org.bdgenomics.adam" %% "adam-distribution" % "0.18.2"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"


