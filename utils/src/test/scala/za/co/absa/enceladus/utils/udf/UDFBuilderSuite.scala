package za.co.absa.enceladus.utils.udf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DoubleType, StructField}
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.types.TypedStructField._
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}

class UDFBuilderSuite extends FunSuite {
  private implicit val defaults: Defaults = GlobalDefaults

  /* TODO Make UDFBuilder work with TypedStructField #1047
  test("Serialization and deserialization of stringUdfViaTypedStructField") {
    val fieldName = "test"
    val field: StructField = StructField(fieldName, LongType, nullable = false)
    val typedField = TypedStructField(field)


    val taggedTypedField = typedField.asInstanceOf[TypedStructFieldTagged[Long]]
    val defaultValue: Option[Long] = typedField.defaultValueWithGlobal.get.map(_.asInstanceOf[Long])
    val udfFnc = UDFBuilder.stringUdfViaTypedStructField(taggedTypedField, fieldName, defaultValue)
    //write
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    out.writeObject(udfFnc)
    out.flush()
    val serialized = bos.toByteArray
    assert(serialized.nonEmpty)
    //read
    val ois = new ObjectInputStream(new ByteArrayInputStream(serialized))
    (ois readObject ()).asInstanceOf[UserDefinedFunction]
  }
  */

  test("Serialization and deserialization of stringUdfViaNumericParser") {
    val fieldName = "test"
    val field: StructField = StructField(fieldName, DoubleType, nullable = false)
    val typedField = TypedStructField(field)


    val numericTypeField = typedField.asInstanceOf[NumericTypeStructField[Double]]
    val defaultValue: Option[Double] = typedField.defaultValueWithGlobal.get.map(_.asInstanceOf[Double])
    val udfFnc = UDFBuilder.stringUdfViaNumericParser(numericTypeField.parser.get, numericTypeField.nullable, fieldName, defaultValue)
    //write
    val bos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(udfFnc)
    oos.flush()
    val serialized = bos.toByteArray
    assert(serialized.nonEmpty)
    //read
    val ois = new ObjectInputStream(new ByteArrayInputStream(serialized))
    (ois readObject ()).asInstanceOf[UserDefinedFunction]
  }
}
