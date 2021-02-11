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

package za.co.absa.enceladus.utils.types

import java.text.ParseException

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField._
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue, ValidationWarning}

import scala.util.{Failure, Success, Try}

class TypedStructFieldSuite extends AnyFunSuite {
  private implicit val defaults: Defaults = GlobalDefaults
  private val fieldName = "test_field"
  private def createField(dataType: DataType,
                          nullable: Boolean = false,
                          default: Option[Any] = None,
                          otherMetadata: Map[String, Any] = Map.empty
                         ): StructField = {
    def addMetadata(builder: MetadataBuilder, key: String, value: Option[Any]): MetadataBuilder = {
      value match {
        case None => builder
        case Some(null) => builder.putNull(key) // scalastyle:ignore null
        case Some(s: String) => builder.putString(key, s)
        case Some(i: Int) => builder.putLong(key, i)
        case Some(l: Long) => builder.putLong(key, l)
        case Some(b: Boolean) => builder.putBoolean(key, b)
        case Some(f: Float) => builder.putDouble(key, f)
        case Some(d: Double) => builder.putDouble(key, d)
        case Some(x) => builder.putString(key, x.toString)
      }
    }
    val metadataBuilder: MetadataBuilder = otherMetadata.foldLeft(new MetadataBuilder()) (
      (builder, data) => addMetadata(builder, key = data._1, value = Some(data._2)))
    val metadata = addMetadata(metadataBuilder, MetadataKeys.DefaultValue, default).build()
    StructField(fieldName, dataType, nullable,metadata)
  }

  def checkField(field: TypedStructField,
                 dataType: DataType,
                 ownDefaultValue: Try[Option[Option[Any]]],
                 defaultValueWithGlobal: Try[Option[Any]],
                 nullable: Boolean = false,
                 validationIssues: Seq[ValidationIssue] = Nil): Unit = {

    def assertTry(got: Try[Any], expected:Try[Any]): Unit = {
      expected match {
        case Success(_) => assert(got == expected)
        case Failure(e) =>
          val caught = intercept[Exception] {
            got.get
          }
          assert(caught.getClass == e.getClass)
          assert(caught.getMessage == e.getMessage)
      }
    }

    assert(field.name == fieldName)
    assert(field.dataType == dataType)
    val (correctType, expectedTypeName) = dataType match {
      case ByteType       => (field.isInstanceOf[ByteTypeStructField], "ByteTypeStructField")
      case ShortType      => (field.isInstanceOf[ShortTypeStructField], "ShortTypeStructField")
      case IntegerType    => (field.isInstanceOf[IntTypeStructField], "IntTypeStructField")
      case LongType       => (field.isInstanceOf[LongTypeStructField], "LongTypeStructField")
      case FloatType      => (field.isInstanceOf[FloatTypeStructField], "FloatTypeStructField")
      case DoubleType     => (field.isInstanceOf[DoubleTypeStructField], "DoubleTypeStructField")
      case StringType     => (field.isInstanceOf[StringTypeStructField], "StringTypeStructField")
      case BinaryType     => (field.isInstanceOf[BinaryTypeStructField], "BinaryTypeStructField")
      case BooleanType    => (field.isInstanceOf[BooleanTypeStructField], "BooleanTypeStructField")
      case DateType       => (field.isInstanceOf[DateTypeStructField], "DateTypeStructField")
      case TimestampType  => (field.isInstanceOf[TimestampTypeStructField], "TimestampTypeStructField")
      case _: DecimalType => (field.isInstanceOf[DecimalTypeStructField], "DecimalTypeStructField")
      case _: ArrayType   => (field.isInstanceOf[ArrayTypeStructField], "ArrayTypeStructField")
      case _: StructType  => (field.isInstanceOf[StructTypeStructField], "StructTypeStructField")
      case _ => (field.isInstanceOf[GeneralTypeStructField], "GeneralTypeStructField")
    }
    assert(correctType, s"\nWrong TypedStructField type. Expected: '$expectedTypeName', but got: '${field.getClass.getSimpleName}'")
    assert(field.nullable == nullable)
    assertTry(field.ownDefaultValue, ownDefaultValue)
    assertTry(field.defaultValueWithGlobal, defaultValueWithGlobal)
    assert(field.validate() == validationIssues)
  }

  test("String type without default defined") {
    val fieldType = StringType
    val field = createField(fieldType)
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(None), Success(Some("")))
  }

  test("Integer type without default defined, nullable") {
    val fieldType = IntegerType
    val nullable = true
    val field = createField(fieldType, nullable)
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(None),  Success(None), nullable)
  }

  test("Double type with default defined, not-nullable") {
    val fieldType = DoubleType
    val nullable = false
    val field = createField(fieldType, nullable, Some("3.14"))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(3.14))), Success(Some(3.14)), nullable)
  }

  test("Date type with default defined as null, nullable") {
    val fieldType = DateType
    val nullable = true
    val field = createField(fieldType, nullable, Some(null)) // scalastyle:ignore null
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(None)), Success(None), nullable)
  }

  test("StructType, not nullable") {
    val innerField = createField(FloatType)
    val fieldType = StructType(Seq(innerField))
    val nullable = false
    val field = createField(fieldType, nullable)
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(None), Failure(new IllegalStateException("No default value defined for data type struct")), nullable)
  }

  test("String type not nullable, with default defined as null") {
    val fieldType = StringType
    val nullable = false
    val field = createField(fieldType, nullable, Some(null)) // scalastyle:ignore null
    val typed = TypedStructField(field)
    val errMsg = s"null is not a valid value for field '$fieldName'"
    val fail = Failure(new IllegalArgumentException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }

  test("Binary type not nullable, with default defined as null") {
    val fieldType = BinaryType
    val nullable = false
    val field = createField(fieldType, nullable, Some(null)) // scalastyle:ignore null
    val typed = TypedStructField(field)
    val errMsg = s"null is not a valid value for field '$fieldName'"
    val warnMsg ="Default value of 'null' found, but no encoding is specified. Assuming 'none'."
    val fail = Failure(new IllegalArgumentException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg), ValidationWarning(warnMsg)))
  }

  test("Byte type not nullable, with default defined as not not-numeric string") {
    val fieldType = ByteType
    val nullable = false
    val field = createField(fieldType, nullable, Some("seven"))
    val typed = TypedStructField(field)
    val errMsg = "'seven' cannot be cast to byte"
    val fail = Failure(new NumberFormatException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }

  test("Long type not nullable, with default defined as binary integer") {
    val fieldType = LongType
    val nullable = false
    val field = createField(fieldType, nullable, Some(-1L))
    val typed = TypedStructField(field)
    val errMsg = "java.lang.Long cannot be cast to java.lang.String"
    val fail = Failure(new ClassCastException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }

  test("Float type nullable, with default defined in exponential notation, allowInfinity is set to true") {
    val fieldType = FloatType
    val nullable = true
    val field = createField(fieldType, nullable, Some("314e-2"), Map(MetadataKeys.AllowInfinity -> "true"))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(3.14F))), Success(Some(3.14F)), nullable)
  }

  test("Boolean type nullable, with default defined as wrong keyword") {
    val fieldType = BooleanType
    val nullable = true
    val field = createField(fieldType, nullable, Some("Nope"))
    val typed = TypedStructField(field)
    val errMsg = "'Nope' cannot be cast to boolean"
    val fail = Failure(new IllegalArgumentException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }

  test("Timestamp type not nullable, with default not adhering to pattern") {
    val fieldType = TimestampType
    val nullable = false
    val field = createField(fieldType, nullable, Some("00:00:00 01.01.2000"), Map("pattern" -> "yyyy-MM-dd HH:mm:ss X"))
    val typed = TypedStructField(field)
    val errMsg = """Unparseable date: "00:00:00 01.01.2000""""
    val fail = Failure(new ParseException(errMsg, 0))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }

  test("Float type nullable, with default defined as Long and allowInfinity as binary Boolean") {
    val fieldType = FloatType
    val nullable = false
    val field = createField(fieldType, nullable, Some(1000L), Map( MetadataKeys.AllowInfinity->false ))
    val typed = TypedStructField(field)
    val errMsg = "java.lang.Long cannot be cast to java.lang.String"
    val fail = Failure(new ClassCastException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(
      ValidationError(errMsg),
      ValidationError(s"${MetadataKeys.AllowInfinity} metadata value of field 'test_field' is not Boolean in String format")
    ))
  }

  test("Decimal type with strictParsing enabled, incorrect value") {
    val fieldType = DecimalType(10, 2)
    val nullable = false
    val field = createField(fieldType, nullable, Some("1000.89899"), Map( MetadataKeys.StrictParsing -> "true" ))
    val typed = TypedStructField(field)
    val errMsg = "'1000.89899' cannot be cast to decimal(10,2)"
    val fail = Failure(new IllegalArgumentException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }
  test("Decimal type with strictParsing enabled, correct value") {
    val fieldType = DecimalType(10, 2)
    val nullable = false
    val field = createField(fieldType, nullable, Some("1000.9"), Map( MetadataKeys.StrictParsing -> "true" ))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(1000.9))), Success(Some(1000.9)), nullable, Seq())
  }

  test("Decimal type with incorrect strictParsing value") {
    val fieldType = DecimalType(10, 2)
    val nullable = false
    val field = createField(fieldType, nullable, Some("1000.889"), Map( MetadataKeys.StrictParsing -> "t" ))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(1000.889))), Success(Some(1000.889)), nullable, Seq(
      ValidationError(s"${MetadataKeys.StrictParsing} metadata value of field 'test_field' is not Boolean in String format"))
    )
  }

  test("Decimal type with false strictParsing") {
    val fieldType = DecimalType(10, 2)
    val nullable = false
    val field = createField(fieldType, nullable, Some("1000.8889"), Map( MetadataKeys.StrictParsing -> "false" ))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(1000.8889))), Success(Some(1000.8889)), nullable, Seq())
  }

  test("Decimal type with no set strictParsing") {
    val fieldType = DecimalType(10, 2)
    val nullable = false
    val field = createField(fieldType, nullable, Some("1000.8889"), Map())
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(1000.8889))), Success(Some(1000.8889)), nullable, Seq())
  }

  test("Array type with long element data type and correct associated metadata") {
    val fieldType = ArrayType(LongType)
    val nullable = true
    val field = createField(fieldType, nullable, Some("9999"), Map( MetadataKeys.MinusSign->"$"))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(None), Success(None), nullable)
  }

  test("Array type with date element data type and incorrect associated metadata") {
    val fieldType = ArrayType(DateType)
    val nullable = true
    val field = createField(fieldType, nullable, None, Map( MetadataKeys.Pattern->"Fubar"))
    val typed = TypedStructField(field)
    val errMsg = "Illegal pattern character 'b'"
    checkField(typed, fieldType, Success(None), Success(None), nullable, Seq(ValidationError(errMsg)))
  }
}
