package za.co.absa.enceladus.utils.types

import java.text.ParseException

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.types.TypedStructField._
import za.co.absa.enceladus.utils.validation.{ValidationError, ValidationIssue}

import scala.util.{Failure, Success, Try}

class TypedStructFieldSuite extends FunSuite {
  private val fieldName = "test_field"
  private def createField(dataType: DataType,
                          nullable: Boolean = false,
                          default: Option[Any] = None,
                          otherMetadata: Map[String, String] = Map.empty
                         ): StructField = {
    val medatadaBuilder: MetadataBuilder = otherMetadata.foldLeft(new MetadataBuilder()) (
      (builder, data) => builder.putString(data._1, data._2)
    )
    val metadata = (default match {
      case None             => medatadaBuilder
      case Some(null)       => medatadaBuilder.putNull(MetadataKeys.DefaultValue) // scalastyle:ignore null
      case Some(s: String)  => medatadaBuilder.putString(MetadataKeys.DefaultValue, s)
      case Some(i: Int)    => medatadaBuilder.putLong(MetadataKeys.DefaultValue, i)
      case Some(l: Long)    => medatadaBuilder.putLong(MetadataKeys.DefaultValue, l)
      case Some(b: Boolean) => medatadaBuilder.putBoolean(MetadataKeys.DefaultValue, b)
      case Some(d: Double)  => medatadaBuilder.putDouble(MetadataKeys.DefaultValue, d)
      case Some(x)          => medatadaBuilder.putString(MetadataKeys.DefaultValue, x.toString)
    }).build()
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
          val cought = intercept[Exception] {
            got.get
          }
          assert(cought.getClass == e.getClass)
          assert(cought.getMessage == e.getMessage)
      }
    }

    assert(field.name == fieldName)
    assert(field.dataType == dataType)
    val (correctType, expectedTypeName) = dataType match {
      case ByteType | ShortType | IntegerType | LongType => (field.isInstanceOf[IntegralTypeStructField], "IntegralTypeStructField")
      case FloatType | DoubleType => (field.isInstanceOf[FractionalTypeStructField], "FractionalTypeStructField")
      case StringType => (field.isInstanceOf[StringTypeStructField], "StringTypeStructField")
      case BooleanType => (field.isInstanceOf[BooleanTypeStructField], "BooleanTypeStructField")
      case DateType | TimestampType => (field.isInstanceOf[DateTimeTypeStructField], "DateTimeTypeStructField")
      case _: DecimalType => (field.isInstanceOf[DecimalTypeStructField], "DecimalTypeStructField")
      case _: ArrayType => (field.isInstanceOf[ArrayTypeStructField], "ArrayTypeStructField")
      case _: StructType => (field.isInstanceOf[StructTypeStructField], "StructTypeStructField")
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

  test("Array type, nullable") {
    val fieldType = ArrayType(IntegerType)
    val nullable = true
    val field = createField(fieldType, nullable)
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(None), Success(None), nullable)
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
    checkField(typed, fieldType, Success(Some(Some(-1L))), Success(Some(-1L)), nullable)
  }

  test("Float type nullable, with default defined in exponential notation") {
    val fieldType = FloatType
    val nullable = true
    val field = createField(fieldType, nullable, Some("314e-2"))
    val typed = TypedStructField(field)
    val errMsg = "'314e-2' cannot be cast to float"
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

  test("Byte type not nullable, with default defined as Double") {
    val fieldType = LongType
    val nullable = false
    val field = createField(fieldType, nullable, Some(3,14))
    val typed = TypedStructField(field)
    val errMsg = "'(3,14)' cannot be cast to long"
    val fail = Failure(new NumberFormatException(errMsg))
    checkField(typed, fieldType, fail, fail, nullable, Seq(ValidationError(errMsg)))
  }

  test("Boolean type nullable, with default defined as Boolean") {
    val fieldType = BooleanType
    val nullable = false
    val field = createField(fieldType, nullable, Some(true))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(true))), Success(Some(true)), nullable)
  }

  test("Float type nullable, with default defined as Long") {
    val fieldType = FloatType
    val nullable = false
    val field = createField(fieldType, nullable, Some(1000L))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(1000D))), Success(Some(1000D)), nullable)
  }

  test("Decimal type nullable, with default defined as binary Boolean") {
    val fieldType = DecimalType(10, 3)
    val nullable = false
    val field = createField(fieldType, nullable, Some(2.71))
    val typed = TypedStructField(field)
    checkField(typed, fieldType, Success(Some(Some(2.71))), Success(Some(2.71)), nullable)
  }
}
