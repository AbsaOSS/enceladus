/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.udf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DecimalType, DoubleType, LongType, MetadataBuilder, ShortType, StructField}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField._
import za.co.absa.enceladus.utils.types.parsers.{DecimalParser, FractionalParser}
import za.co.absa.enceladus.utils.types.parsers.IntegralParser.{PatternIntegralParser, RadixIntegralParser}
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}

class UDFBuilderSuite extends AnyFunSuite {
  private implicit val defaults: Defaults = GlobalDefaults

  test("Serialization and deserialization of stringUdfViaNumericParser (FractionalParser)") {
    val fieldName = "test"
    val field: StructField = StructField(fieldName, DoubleType, nullable = false)
    val typedField = TypedStructField(field)


    val numericTypeField = typedField.asInstanceOf[NumericTypeStructField[Double]]
    val defaultValue: Option[Double] = typedField.defaultValueWithGlobal.get.map(_.asInstanceOf[Double])
    val parser = numericTypeField.parser.get.asInstanceOf[FractionalParser[Double]]
    val udfFnc = UDFBuilder.stringUdfViaNumericParser(parser, numericTypeField.nullable, fieldName, defaultValue)
    //write
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(udfFnc)
    oos.flush()
    val serialized = baos.toByteArray
    assert(serialized.nonEmpty)
    //read
    val ois = new ObjectInputStream(new ByteArrayInputStream(serialized))
    (ois readObject ()).asInstanceOf[UserDefinedFunction]
  }

  test("Serialization and deserialization of stringUdfViaNumericParser (DecimalParser)") {
    val fieldName = "test"
    val field: StructField = StructField(fieldName, DecimalType(20,5), nullable = false)
    val typedField = TypedStructField(field)


    val numericTypeField = typedField.asInstanceOf[NumericTypeStructField[BigDecimal]]
    val defaultValue = typedField.defaultValueWithGlobal.get.map(_.asInstanceOf[BigDecimal])
    val parser = numericTypeField.parser.get.asInstanceOf[DecimalParser]
    val udfFnc = UDFBuilder.stringUdfViaNumericParser(parser, numericTypeField.nullable, fieldName, defaultValue)
    //write
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(udfFnc)
    oos.flush()
    val serialized = baos.toByteArray
    assert(serialized.nonEmpty)
    //read
    val ois = new ObjectInputStream(new ByteArrayInputStream(serialized))
    (ois readObject ()).asInstanceOf[UserDefinedFunction]
  }

  test("Serialization and deserialization of stringUdfViaNumericParser (RadixIntegralParser)") {
    val fieldName = "test"
    val field: StructField = StructField(fieldName, LongType, nullable = false, new MetadataBuilder()
      .putString(MetadataKeys.Radix, "hex")
      .putString(MetadataKeys.DefaultValue, "FF")
      .build)
    val typedField = TypedStructField(field)


    val numericTypeField = typedField.asInstanceOf[NumericTypeStructField[Long]]
    val defaultValue: Option[Long] = typedField.defaultValueWithGlobal.get.map(_.asInstanceOf[Long])
    val parser = numericTypeField.parser.get.asInstanceOf[RadixIntegralParser[Long]]
    val udfFnc = UDFBuilder.stringUdfViaNumericParser(parser, numericTypeField.nullable, fieldName, defaultValue)
    //write
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(udfFnc)
    oos.flush()
    val serialized = baos.toByteArray
    assert(serialized.nonEmpty)
    //read
    val ois = new ObjectInputStream(new ByteArrayInputStream(serialized))
    (ois readObject ()).asInstanceOf[UserDefinedFunction]
  }

  test("Serialization and deserialization of stringUdfViaNumericParser (PatternIntegralParser)") {
    val fieldName = "test"
    val field: StructField = StructField(fieldName, ShortType, nullable = true, new MetadataBuilder()
      .putString(MetadataKeys.Pattern, "0 feet")
      .build)
    val typedField = TypedStructField(field)


    val numericTypeField = typedField.asInstanceOf[NumericTypeStructField[Short]]
    val defaultValue: Option[Short] = typedField.defaultValueWithGlobal.get.map(_.asInstanceOf[Short])
    val parser = numericTypeField.parser.get.asInstanceOf[PatternIntegralParser[Short]]
    val udfFnc = UDFBuilder.stringUdfViaNumericParser(parser, numericTypeField.nullable, fieldName, defaultValue)
    //write
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(udfFnc)
    oos.flush()
    val serialized = baos.toByteArray
    assert(serialized.nonEmpty)
    //read
    val ois = new ObjectInputStream(new ByteArrayInputStream(serialized))
    (ois readObject ()).asInstanceOf[UserDefinedFunction]
  }

}
