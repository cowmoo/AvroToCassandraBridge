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

import com.datastax.driver.core.schemabuilder.CreateType

import com.datastax.driver.core.DataType

case class AvroType(schema:Schema, isNullable:Boolean, isUDT:Boolean) {
  def cassandraType():DataType = {

    /*def debugDataType(dType:Type) = {
      println(dType)
      DataType.blob()
    }*/

    def atomicType(atomicSchema:Schema):DataType = {
      atomicSchema.getType() match {
        case Type.STRING => DataType.text()
        case Type.INT => DataType.cint()
        case Type.LONG => DataType.bigint()
        case Type.BOOLEAN => DataType.cboolean()
        case Type.FLOAT => DataType.cfloat()
        case _ => DataType.text()
      }
    }

    schema.getType() match {
        case Type.ARRAY => DataType.list(atomicType(schema.getElementType()))
        case _ => atomicType(schema)
    }
  }
}

object AvroType {
  def get(field:Field):Option[AvroType] = {
    getSchema(field.schema())
  }

  def getSchema(fieldSchema:Schema):Option[AvroType] = {
    val udt = isUDT(fieldSchema)

    getNullableSchema(fieldSchema) match {
      case (true, schema:Schema) => return Some(AvroType(schema, true, udt))
      case (false, schema:Schema) => return Some(AvroType(schema, false, udt))
      case _ => return None
    }
  }

  def getNullableSchema(schema:Schema):Any = {
    if (schema.getType().equals(Type.UNION) &&
      schema.getTypes().size() == 2 &&
      schema.getTypes().map(t => t.getType()).contains(Type.NULL))
      return (true, schema.getTypes().find(f => !f.getType().equals(Type.NULL)).get)
    else
      return (false, schema)
  }

  def isUDT(schema:Schema):Boolean = {
    if (schema.getType().equals(Type.UNION))
      return schema.getTypes().map(t => t.getType()).contains(Type.RECORD)
    else
      return false
  }

  def isSubType(avroType:AvroType):Boolean = {
    return avroType.isUDT
  }
}

object AvroToCassandraTranslator {

  val udtList = List[AvroType]()

  /*def handleUDT(udtType:AvroType):List = {
    val schemaAndField = List()

    if (udtList.contains(udtType)) {
      val typeDeclaration = SchemaBuilder.createType(schema.getName())
      udtType.schema.getFields().foreach(f => processField(f, typeDeclaration))
    }
  }

  def processField(field:Field, cassandraCreate:CreateType): Any = {
    AvroType.get(f) match {
      case Some(avroType) if avroType.isUDT => _
      case Some(avroType) => println(f.)
      case _ => return None
    }
  }*/

  def main(args:Array[String]) {

    val schema = new Parser().parse(new FileInputStream("/sandbox/Genotype.avsc"))

    val createTable = SchemaBuilder.createTable(schema.getName())
                        .addPartitionKey("id", DataType.text())

    schema.getFields().map(f =>
      AvroType.get(f) match {
        case Some(avroType) if avroType.isUDT => ()
        case Some(avroType) => createTable.addColumn(f.name(), avroType.cassandraType())
        case _ => ()
      }
    )

    println(createTable.getQueryString())

      /*NullableType.get(f) match {
        case Some(fieldSchema) => println(f.name())
        case _ => ()
      }
    )*/

    // create a traversal function
    // to traverse fields
    // convert fields to a NullableType
    // if NullableType is a custom UdtType and doesn't exist yet
      // create the NullableType
    // convert the field to Cassandra Type
    // get it to register to Cassandra memory
  }
}
