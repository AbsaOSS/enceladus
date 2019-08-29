/*
 * Copyright 2018-2019 ABSA Group Limited
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

import org.apache.spark.sql.types._
import za.co.absa.enceladus.utils.implicits.StructFieldImplicits.StructFieldEnhancements
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.time.DateTimePattern
import za.co.absa.enceladus.utils.types.parsers._
import za.co.absa.enceladus.utils.validation.ValidationIssue
import za.co.absa.enceladus.utils.validation.field.{DateFieldValidator, DateTimeFieldValidator, FieldValidator, ScalarFieldValidator, TimestampFieldValidator}

import scala.util.{Failure, Success, Try}

sealed abstract class TypedStructField(structField: StructField) extends StructFieldEnhancements(structField) {
  protected def convertString(string: String): Try[Any]

  def validate(): Seq[ValidationIssue]

  def stringToTyped(string: String): Try[Option[Any]] = {
    def errMsg: String = {
      s"'$string' cannot be cast to ${dataType.typeName}"
    }

    if (string == null) {
      if (structField.nullable) {
        Success(None)
      } else {
        Failure(new IllegalArgumentException(s"null is not a valid value for field '${structField.name}'"))
      }
    } else {
      convertString(string) match {
        case Failure(e: NumberFormatException) if e.getClass == classOf[NumberFormatException] =>
          // replacing some not very informative exception message with better one
          Failure(new NumberFormatException(errMsg))
        case Failure(e: IllegalArgumentException)  if e.getClass == classOf[IllegalArgumentException]=>
          // replacing some not very informative exception message with better one
          Failure(new IllegalArgumentException(errMsg, e.getCause))
        case Failure(e) =>
          // other failures stay unchanged
          Failure(e)
        case Success(good) =>
          // good result is put withing the option as the return type requires
          Success(Some(good))
      }
    }

  }

  /**
   * The default value defined in the metadata of the field, if present
   * @return  Try - because the gathering may fail in conversion between types
   *          outer Option - None means no default was defined within the metadata of the field
   *          inner Option - the actual default value or None in case the default is null
   */
  def ownDefaultValue: Try[Option[Option[Any]]] = {
    if (hasMetadataKey(MetadataKeys.DefaultValue)) {
      for {
        defaultValueString <- Try{structField.metadata.getString(MetadataKeys.DefaultValue)}
        defaultValueTyped <- stringToTyped(defaultValueString)
      } yield Some(defaultValueTyped)
    } else {
      Success(None)
    }
  }

  /**
   * The default value that will be used for the field, local if defined otherwise global
   * @return Try - because the gathering of local default  may fail in conversion between types
   *         Option - the actual default value or None in case the default is null
   */
  def defaultValueWithGlobal: Try[Option[Any]] = {
    for {
      localDefault <- ownDefaultValue
      result <- localDefault match {
        case Some(value) => Success(value)
        case None => Defaults.getGlobalDefaultWithNull(dataType, nullable)
      }
    } yield result
  }

  def pattern: Try[Option[TypePattern]] = {
    Success(None)
  }

  def name: String = structField.name
  def nullable: Boolean = structField.nullable
  def dataType: DataType = structField.dataType

  def canEqual(any: Any): Boolean = any.isInstanceOf[TypedStructField]

  override def equals(other: Any): Boolean = other match {
    case that: TypedStructField => that.canEqual(this) && structField == that.structField
    case _ => false
  }

  override def hashCode(): Int = {
    /* one of the suggested ways to implement the hasCode logic */
    val prime = 31
    var result = 1
    result = prime * result + (if (structField == null) 0 else structField.hashCode)
    result
  }
}

object TypedStructField {
  /**
   * This is to be the only accessible constructor for TypedStructField sub-classes
   * The point is, that sub-classes have private constructors to prevent their instantiation outside this apply
   * constructor. This is to ensure at compile time there is a bound between the provided StructField.dataType and the
   * class created
   * @param structField the structField to wrap TypedStructField around
   * @return            the object of non-abstract TypedStructField successor class relevant to the StructField dataType
   */
  def apply(structField: StructField): TypedStructField = {
    structField.dataType match {
      case _: StringType    => new StringTypeStructField(structField)
      case _: BooleanType   => new BooleanTypeStructField(structField)
      case _: ByteType      => new IntegralTypeStructField(structField)
      case _: ShortType     => new IntegralTypeStructField(structField)
      case _: IntegerType   => new IntegralTypeStructField(structField)
      case _: LongType      => new IntegralTypeStructField(structField)
      case _: FloatType     => new FractionalTypeStructField(structField)
      case _: DoubleType    => new FractionalTypeStructField(structField)
      case dt: DecimalType  => new DecimalTypeStructField(structField, dt)
      case _: TimestampType => new DateTimeTypeStructField(structField, TimestampFieldValidator)
      case _: DateType      => new DateTimeTypeStructField(structField, DateFieldValidator)
      case at: ArrayType    => new ArrayTypeStructField(structField, at)
      case st: StructType   => new StructTypeStructField(structField, st)
      case _                => new GeneralTypeStructField(structField)
    }
  }

  def asStringTypedStructField(structField: StructField): StringTypeStructField = TypedStructField(structField).asInstanceOf[StringTypeStructField]
  def asBooleanTypeStructField(structField: StructField): BooleanTypeStructField = TypedStructField(structField).asInstanceOf[BooleanTypeStructField]
  def asIntegralTypeStructField(structField: StructField): IntegralTypeStructField = TypedStructField(structField).asInstanceOf[IntegralTypeStructField]
  def asFractionalTypeStructField(structField: StructField): FractionalTypeStructField = TypedStructField(structField).asInstanceOf[FractionalTypeStructField]
  def asDecimalTypeStructField(structField: StructField): DecimalTypeStructField = TypedStructField(structField).asInstanceOf[DecimalTypeStructField]
  def asDateTimeTypeStructField(structField: StructField): DateTimeTypeStructField = TypedStructField(structField).asInstanceOf[DateTimeTypeStructField]
  def asArrayTypeStructField(structField: StructField): ArrayTypeStructField = TypedStructField(structField).asInstanceOf[ArrayTypeStructField]
  def asStructTypeStructField(structField: StructField): StructTypeStructField = TypedStructField(structField).asInstanceOf[StructTypeStructField]

  def unapply(typedStructField: TypedStructField): Option[StructField] = Some(typedStructField.structField)

  final class StringTypeStructField private[TypedStructField] (structField: StructField) extends TypedStructField(structField) {
    override protected def convertString(string: String): Try[Any] = {
      Success(string)
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }
  }

  final class BooleanTypeStructField private[TypedStructField] (structField: StructField) extends TypedStructField(structField) {
    override protected def convertString(string: String): Try[Any] = {
      Try{string.toBoolean}
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }
  }

  final class IntegralTypeStructField private[TypedStructField] (structField: StructField) extends TypedStructField(structField) {
    val parser: IntegralParser = IntegralParser

    override protected def convertString(string: String): Try[Any] = {
      Try{
        dataType match {
          case ByteType => parser.parseByte(string)
          case ShortType => parser.parseShort(string)
          case IntegerType => parser.parseInt(string)
          case LongType => parser.parseLong(string)
        }
      }
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }
  }

  final class FractionalTypeStructField private[TypedStructField] (structField: StructField) extends TypedStructField(structField) {

    val allowInfinity: Boolean = getMetadataBoolean(MetadataKeys.allowInfinity).getOrElse(false)

    val parser: FractionalParser = FractionalParser(allowInfinity)

    override protected def convertString(string: String): Try[Any] = {
      Try{
        dataType match {
          case FloatType => parser.parseFloat(string)
          case DoubleType => parser.parseDouble(string)
        }
      }
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }
  }

  final class DecimalTypeStructField private[TypedStructField] (structField: StructField, override val dataType: DecimalType) extends TypedStructField(structField) {
    val parser = DecimalParser(precision, scale)

    override protected def convertString(string: String): Try[Any] = {
      Try{parser.parseDecimal(string)}
    }

    override def validate(): Seq[ValidationIssue] = {
      ScalarFieldValidator.validate(this)
    }

    def precision: Int = dataType.precision
    def scale: Int = dataType.scale
  }

  final class DateTimeTypeStructField private[TypedStructField] (structField: StructField, validator: DateTimeFieldValidator)
    extends TypedStructField(structField) {

    override def pattern: Try[Option[DateTimePattern]] = {
      parser.map(x => Some(x.pattern))
    }

    lazy val parser: Try[DateTimeParser] = {
      val patternOpt: Option[String] = getMetadataString(MetadataKeys.Pattern)
      val patternToUse = patternOpt match {
        case Some(pattern) =>
          val timeZoneOpt = getMetadataString(MetadataKeys.DefaultTimeZone)
          DateTimePattern(pattern, timeZoneOpt)
        case None =>
          DateTimePattern.asDefault(Defaults.getGlobalFormat(dataType), None)
      }
      Try{
        DateTimeParser(patternToUse)
      }
    }

    override protected def convertString(string: String): Try[Any] = {
      dataType match {
        case TimestampType => parser.map(_.parseTimestamp(string))
        case DateType => parser.map(_.parseDate(string))
      }
    }

    override def validate(): Seq[ValidationIssue] = {
      validator.validate(this)
    }
  }

  sealed trait WeakSupport {
    this: TypedStructField =>

    def structField: StructField

    def convertString(string: String): Try[Any] = {
      Failure(new IllegalStateException(s"No converter defined for data type ${structField.dataType.typeName}"))
    }

    def validate(): Seq[ValidationIssue] = {
      FieldValidator.validate(this)
    }
  }

  final class ArrayTypeStructField private[TypedStructField] (structField: StructField, override val dataType: ArrayType)
    extends TypedStructField(structField)
    with WeakSupport

  final class StructTypeStructField(structField: StructField, override val dataType: StructType)
    extends TypedStructField(structField)
    with WeakSupport

  final class GeneralTypeStructField private[TypedStructField] (structField: StructField) extends TypedStructField(structField) with WeakSupport

}
