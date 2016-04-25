import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

import org.apache.avro.Schema

import org.apache.avro.Schema.Parser

import org.apache.avro.Schema.Field

import java.io.FileInputStream

import scala.collection.JavaConversions._

import com.datastax.driver.core.schemabuilder.SchemaBuilder

import com.datastax.driver.core.DataType

import com.datastax.driver.core.DataType.Name

import org.apache.avro.Schema.Type

object CassandraConnector {
  def main(args:Array[String]) {
    //val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1")
    
    
   //val conf = new SparkConf().setAppName("Simple Application")
    //val sc = new SparkContext(conf)
    //val rdd = sc.cassandraTable("test", "kv")
    //println(rdd.count)
    //println(rdd.first)
    //println(rdd.map(_.getInt("value")).sum)

    def processFields(field:Field, createTable:com.datastax.driver.core.schemabuilder.Create) = {
      val data = convertData(field.schema())
      data match {
        case dataType:com.datastax.driver.core.DataType => createTable.addColumn(field.name(), dataType)
        case udtType:com.datastax.driver.core.schemabuilder.UDTType => createTable.addUDTColumn(field.name(), udtType)
      }
    }

    def processFieldsUDTType(field:Field, createType:com.datastax.driver.core.schemabuilder.CreateType) = {
      val data = convertData(field.schema())

      data match {
        case dataType:com.datastax.driver.core.DataType => createType.addColumn(field.name(), dataType)
        case udtType:com.datastax.driver.core.schemabuilder.UDTType => createType.addUDTColumn(field.name(), udtType)
      }
    }

    def convertData(schema:Schema):Any = {

      val typeStrs = if (schema.getType().equals(Type.UNION)) {
        schema.getTypes().map(t => t.getType())
      } else {
        List("")
      }

      //if (schema.getType().equals(Type.RECORD)) {
      if (schema.getType().equals(Type.RECORD)) {
        val udt = SchemaBuilder.frozen(schema.getName())

        val typeDeclaration = SchemaBuilder.createType(schema.getName())

        schema.getFields().foreach(f => processFieldsUDTType(f, typeDeclaration))

        println(typeDeclaration.getQueryString())

        return udt
      } else if (typeStrs.contains(Type.RECORD)) {

        val recordSchema = schema.getTypes().find(f => f.getType().equals(Type.RECORD)).get

        val typeDeclaration = SchemaBuilder.createType(recordSchema.getName())

        recordSchema.getFields().foreach(f => processFieldsUDTType(f, typeDeclaration))

        println(typeDeclaration.getQueryString())

        return SchemaBuilder.frozen(recordSchema.getName())

      } else if (schema.getType().equals(Type.ARRAY)) {
        if (schema.getElementType().getType().equals(Type.STRING))
          return DataType.list(DataType.text())
        else if (schema.getElementType().getType().equals(Type.INT))
          return DataType.list(DataType.cint())
      }
      else if (!schema.getType().equals(Type.UNION)) {
        return DataType.text()
      }

      //val typeStrs = schema.getTypes().map(typeStr => typeStr.getType())

      if (typeStrs.contains(Type.STRING)) {
        return DataType.text()
      } else if (typeStrs.contains(Type.INT)) {
        return DataType.cint()
      } else if (typeStrs.contains(Type.LONG)) {
        return DataType.bigint()
      } else if (typeStrs.contains(Type.BOOLEAN)) {
        return DataType.cboolean()
      } else if (typeStrs.contains(Type.FLOAT)) {
        return DataType.cfloat()
      } else {
        return DataType.text()
      }
    }

    val schema = new Parser().parse(new FileInputStream("/sandbox/Genotype.avsc"))

    val createTable = SchemaBuilder.createTable(schema.getName())
                        .addPartitionKey("id", DataType.text())
                        //.addClusteringColumn("id", DataType.text())
                      //.addPartitionKey("id", DataType.text())

    schema.getFields().foreach(field => processFields(field, createTable))

    println(createTable.getQueryString())

    /*  .
      .addPartitionKey("id", DataType.text())
      .addPartitionKey("testEnum", DataType.text());

    schema.getFields().map(field => field.name()).foreach(name => println(name))*/

    /*  .map(columns => columns.split("\t") )

    val field = schema.getFields().get(2)

    val typeStr = field.schema().getTypes().get(1).getType()

    println(typeStr)*/

  }
}
