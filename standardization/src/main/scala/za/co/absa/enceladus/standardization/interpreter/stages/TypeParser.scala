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

package za.co.absa.enceladus.standardization.interpreter.stages

import java.security.InvalidParameterException
import java.sql.Timestamp
import java.util.Date
import java.util.regex.Pattern

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.schema.SchemaUtils.appendPath
import za.co.absa.enceladus.utils.types.{Defaults, TypedStructField}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.spark.hofs.transform
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.time.DateTimePattern
import za.co.absa.enceladus.utils.typeClasses.{DoubleLike, LongLike}
import za.co.absa.enceladus.utils.types.TypedStructField._
import za.co.absa.enceladus.utils.udf.UDFBuilder

import scala.reflect.runtime.universe._
import scala.util.Random

/**
  * Base trait for standardization function
  * Each final class in the hierarchy represents a `standardize` function for its specific data type field
  * Class hierarchy:
  *   TypeParser
  *     ArrayParser !
  *     StructParser !
  *     PrimitiveParser
  *       ScalarParser
  *         NumericParser !
  *         StringParser !
  *         BooleanParser !
  *       DateTimeParser
  *         TimestampParser !
  *         DateParser !
  */
sealed trait TypeParser[T] {
  def standardize(): ParseOutput

  protected val origSchema: StructType
  protected val field: TypedStructField
  protected val metadata: Metadata = field.structField.metadata
  protected val path: String
  protected val parent: Option[TypeParser.Parent]
  protected val fieldInputName: String = SchemaUtils.getFieldNameOverriddenByMetadata(field.structField)
  protected val fieldOutputName: String = field.name
  protected val inputFullPathName: String = appendPath(path, fieldInputName)
  protected val column: Column = parent.map(_.childColumn(fieldInputName)).getOrElse(col(inputFullPathName)) // no parent
  protected val isArrayElement: Boolean = parent.exists(_.isInstanceOf[TypeParser.ArrayParent])
  protected val columnIdForUdf: String = if (isArrayElement) {
      s"$inputFullPathName[*]"
    } else {
      inputFullPathName
    }
}

object TypeParser {
  import za.co.absa.enceladus.utils.implicits.ColumnImplicits.ColumnEnhancements

  private val decimalType = DecimalType(30,9) // scalastyle:ignore magic.number
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val MillisecondsPerSecond = 1000
  private val MicrosecondsPerSecond = 1000000
  private val NanosecondsPerSecond  = 1000000000
  private val InfinityStr = "Infinity"


  def standardize(field: StructField, path: String, origSchema: StructType)
                 (implicit udfLib: UDFLibrary, defaults: Defaults): ParseOutput = {
    // udfLib implicit is present for error column UDF implementation
    TypeParser(field, path, origSchema).standardize()
  }

  sealed trait Parent {
    val parentColumn: Column
    def childColumn(fieldName: String): Column
  }

  private final case class ArrayParent (parentColumn: Column) extends Parent {
    override def childColumn(fieldName: String): Column = parentColumn
  }
  private final case class StructParent (parentColumn: Column) extends Parent {
    override def childColumn(fieldName: String): Column = parentColumn(fieldName)
  }

  private def apply(field: StructField,
                    path: String,
                    origSchema: StructType,
                    parent: Option[Parent] = None)
                   (implicit defaults: Defaults): TypeParser[_] = {
    val parserClass: (String, StructType, Option[Parent]) => TypeParser[_] = field.dataType match {
      case _: ArrayType     => ArrayParser(TypedStructField.asArrayTypeStructField(field), _, _, _)
      case _: StructType    => StructParser(TypedStructField.asStructTypeStructField(field), _, _, _)
      case _: ByteType      =>
        IntegralParser(TypedStructField.asNumericTypeStructField[Byte](field), _, _, _, Set(ShortType, IntegerType, LongType))
      case _: ShortType     => IntegralParser(TypedStructField.asNumericTypeStructField[Short](field), _, _, _, Set(IntegerType, LongType))
      case _: IntegerType   => IntegralParser(TypedStructField.asNumericTypeStructField[Int](field), _, _, _, Set(LongType))
      case _: LongType      => IntegralParser(TypedStructField.asNumericTypeStructField[Long](field), _, _, _, Set.empty)
      case _: FloatType     => FractionalParser(TypedStructField.asNumericTypeStructField[Float](field), _, _, _)
      case _: DoubleType    => FractionalParser(TypedStructField.asNumericTypeStructField[Double](field), _, _, _)
      case _: DecimalType   => DecimalParser(TypedStructField.asNumericTypeStructField[BigDecimal](field), _, _, _)
      case _: StringType    => StringParser(TypedStructField(field), _, _, _)
      case _: BooleanType   => BooleanParser(TypedStructField(field), _, _, _)
      case _: DateType      => DateParser(TypedStructField.asDateTimeTypeStructField(field), _, _, _)
      case _: TimestampType => TimestampParser(TypedStructField.asDateTimeTypeStructField(field), _, _, _)
      case t                => throw new IllegalStateException(s"${t.typeName} is not a supported type in this version of Enceladus")
    }
    parserClass(path, origSchema, parent)
  }

  private final case class ArrayParser(override val field: ArrayTypeStructField,
                                       path: String,
                                       origSchema: StructType,
                                       parent: Option[Parent])
                                      (implicit defaults: Defaults) extends TypeParser[Any] {
    private val fieldType = field.dataType
    private val arrayField = StructField(fieldInputName, fieldType.elementType, fieldType.containsNull)

    override def standardize(): ParseOutput = {
      logger.info(s"Creating standardization plan for Array $inputFullPathName")

      val lambdaVariableName = s"${SchemaUtils.unpath(inputFullPathName)}_${Random.nextLong().abs}"
      val lambda = (forCol: Column) => TypeParser(arrayField, path, origSchema, Option(ArrayParent(forCol)))
        .standardize()

      val lambdaErrCols = lambda.andThen(_.errors)
      val lambdaStdCols = lambda.andThen(_.stdCol)
      val nullErrCond = column.isNull and lit(!field.nullable)

      val finalErrs = when(nullErrCond,
        array(typedLit(ErrorMessage.stdNullErr(inputFullPathName))))
        .otherwise(
          typedLit(flatten(transform(column, lambdaErrCols, lambdaVariableName)))
        )
      val stdCols = transform(column, lambdaStdCols, lambdaVariableName)
      logger.info(s"Finished standardization plan for Array $inputFullPathName")
      ParseOutput(stdCols as (fieldOutputName, metadata), finalErrs)
    }
  }

  private final case class StructParser(override val field: StructTypeStructField,
                                        path: String,
                                        origSchema: StructType,
                                        parent: Option[Parent])
                                       (implicit defaults: Defaults) extends TypeParser[Any] {
    private val fieldType = field.dataType

    override def standardize(): ParseOutput = {
      val out =  fieldType.fields.map(TypeParser(_, inputFullPathName, origSchema, Option(StructParent(column)))
        .standardize())
      val cols = out.map(_.stdCol)
      val errs = out.map(_.errors)
      // condition for nullable error of the struct itself
      val nullErrCond = column.isNull and lit(!field.nullable)
      val dropChildrenErrsCond = column.isNull
      // first remove all child errors if this is null
      val errs1 = concat(
        flatten(array(errs.map(x => when(dropChildrenErrsCond, typedLit(Seq[ErrorMessage]())).otherwise(x)): _*)),
        // then add an error if this one is null
        when(nullErrCond,
          array(callUDF("stdNullErr", lit(inputFullPathName))))
          .otherwise(
            typedLit(Seq[ErrorMessage]())
          )
      )
      // rebuild the struct
      val outputColumn = struct(cols: _*) as (fieldOutputName, metadata)

      ParseOutput(outputColumn, errs1)
    }
  }

  private abstract class PrimitiveParser[T](implicit defaults: Defaults) extends TypeParser[T] {
    // Error should never appear here due to validation
    protected val defaultValue: Option[field.BaseType] = field.defaultValueWithGlobal.get

    override def standardize(): ParseOutput = {
      val castedCol: Column = assemblePrimitiveCastLogic
      val castHasError: Column = assemblePrimitiveCastErrorLogic(castedCol)

      val err: Column  = if (field.nullable) {
        when(column.isNotNull and castHasError, // cast failed
          array(callUDF("stdCastErr", lit(columnIdForUdf), column.cast(StringType)))
        ).otherwise( // everything is OK
          typedLit(Seq.empty[ErrorMessage])
        )
      } else {
        when(column.isNull, // NULL not allowed
          array(callUDF("stdNullErr", lit(columnIdForUdf)))
        ).otherwise( when(castHasError, // cast failed
          array(callUDF("stdCastErr", lit(columnIdForUdf), column.cast(StringType)))
        ).otherwise( // everything is OK
          typedLit(Seq.empty[ErrorMessage])
        ))
      }

      val std: Column = when(size(err) > lit(0), // there was an error on cast
        defaultValue.orNull // converting Option to value or Null
      ).otherwise( when (column.isNotNull,
        castedCol
      ) //.otherwise(null) - no need to explicitly mention
      ) as (fieldOutputName, metadata)

      ParseOutput(std, err)
    }

    protected def assemblePrimitiveCastLogic: Column //this differs based on the field data type

    protected def assemblePrimitiveCastErrorLogic(castedCol: Column): Column = {
      castedCol.isNull  //this one is sufficient for most primitive data types
    }

    @NoSuchElementException(s"The type of '$inputFullPathName' cannot be determined")
    protected def origType: DataType = {
      SchemaUtils.getFieldType(inputFullPathName, origSchema).get
    }

  }

  private abstract class ScalarParser[T](implicit defaults: Defaults) extends PrimitiveParser[T] {
    override def assemblePrimitiveCastLogic: Column = column.cast(field.dataType)
  }

  private abstract class NumericParser[N: TypeTag](override val field: NumericTypeStructField[N])
                                                  (implicit defaults: Defaults) extends ScalarParser[N] {
    override def standardize(): ParseOutput = {
      if (field.needsUdfParsing) {
        standardizeUsingUdf()
      } else {
        super.standardize()
      }
    }

    override def assemblePrimitiveCastLogic: Column = {
      if (origType == StringType) {
        // in case of string as source some adjustments might be needed
        val decimalSymbols = field.pattern.toOption.flatten.map(
          _.decimalSymbols
        ).getOrElse(defaults.getDecimalSymbols)
        val replacements: Map[Char, Char] = decimalSymbols.basicSymbolsDifference(defaults.getDecimalSymbols)

        val columnWithProperDecimalSymbols: Column = if (replacements.nonEmpty) {
          val from = replacements.keys.mkString
          val to = replacements.values.mkString
          translate(column, from, to)
        } else {
          column
        }

        val columnToCast = if (field.allowInfinity && (decimalSymbols.infinityValue != InfinityStr)) {
          // because Spark uses Java's conversion from String, which in turn checks for hardcoded "Infinity" string not
          // DecimalFormatSymbols content
          val infinityEscaped = Pattern.quote(decimalSymbols.infinityValue)
          regexp_replace(regexp_replace(columnWithProperDecimalSymbols, InfinityStr, s"${InfinityStr}_"), infinityEscaped, InfinityStr)
        } else {
          columnWithProperDecimalSymbols
        }
        columnToCast.cast(field.dataType)
      } else {
        super.assemblePrimitiveCastLogic
      }
    }

    private def standardizeUsingUdf(): ParseOutput = {
      val udfFnc: UserDefinedFunction = UDFBuilder.stringUdfViaNumericParser(field.parser.get, field.nullable, columnIdForUdf, defaultValue)
      ParseOutput(udfFnc(column)("result").cast(field.dataType).as(fieldOutputName), udfFnc(column)("error"))
    }
  }

  private final case class IntegralParser[N: LongLike: TypeTag](override val field: NumericTypeStructField[N],
                                                                path: String,
                                                                origSchema: StructType,
                                                                parent: Option[Parent],
                                                                overflowableTypes: Set[DataType])
                                                               (implicit defaults: Defaults) extends NumericParser[N](field) {
    override protected def assemblePrimitiveCastErrorLogic(castedCol: Column): Column = {
      val basicLogic: Column = super.assemblePrimitiveCastErrorLogic(castedCol)

      origType match {
        case  dt: DecimalType =>
          // decimal can be too big, to catch overflow or imprecision  issues compare to original
          basicLogic or (column =!= castedCol.cast(dt))
        case DoubleType | FloatType =>
          // same as Decimal but directly comparing fractional values is not reliable,
          // best check for whole number is considered modulo 1.0
          basicLogic or (column % 1.0 =!= 0.0) or column > field.typeMax or column < field.typeMin
        case ot if overflowableTypes.contains(ot) =>
          // from these types there is the possibility of under-/overflow, extra check is needed
          basicLogic or (castedCol =!= column.cast(LongType))
        case StringType =>
          // string of decimals are not allowed
          basicLogic or column.contains(".")
        case _ =>
          basicLogic
      }
    }
  }

  private final case class DecimalParser(override val field: NumericTypeStructField[BigDecimal],
                                         path: String,
                                         origSchema: StructType,
                                         parent: Option[Parent])
                                        (implicit defaults: Defaults)
    extends NumericParser[BigDecimal](field)
    // NB! loss of precision is not addressed for any DecimalType
    // e.g. 3.141592 will be Standardized to Decimal(10,2) as 3.14

  private final case class FractionalParser[N: DoubleLike: TypeTag](override val field: NumericTypeStructField[N],
                                                                    path: String,
                                                                    origSchema: StructType,
                                                                    parent: Option[Parent])
                                                                   (implicit defaults: Defaults)
    extends NumericParser[N](field) {
    override protected def assemblePrimitiveCastErrorLogic(castedCol: Column): Column = {
      //NB! loss of precision is not addressed for any fractional type
      if (field.allowInfinity) {
        castedCol.isNull or castedCol.isNaN
      } else {
        castedCol.isNull or castedCol.isNaN or castedCol.isInfinite
      }
    }
  }

  private final case class StringParser(field: TypedStructField,
                                        path: String,
                                        origSchema: StructType,
                                        parent: Option[Parent])
                                       (implicit defaults: Defaults) extends ScalarParser[String]

  private final case class BooleanParser(field: TypedStructField,
                                         path: String,
                                         origSchema: StructType,
                                         parent: Option[Parent])
                                        (implicit defaults: Defaults) extends ScalarParser[Boolean]

  /**
    * Timestamp conversion logic
    * Original type | TZ in pattern/without TZ        | Has default TZ
    * ~~~~~~~~~~~~~~|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    * Fractional    | ->Decimal->String->to_timestamp | ->Decimal->String->to_timestamp->to_utc_timestamp
    * Decimal       | ->String->to_timestamp          | ->String->to_timestamp->to_utc_timestamp
    * String        | ->to_timestamp                  | ->to_timestamp->to_utc_timestamp
    * Timestamp     | O                               | ->to_utc_timestamp
    * Date          | ->to_timestamp(no pattern)      | ->to_utc_timestamp
    * Other         | ->String->to_timestamp          | ->String->to_timestamp->to_utc_timestamp
    *
    *
    * Date conversion logic
    * Original type | TZ in pattern/without TZ        | Has default TZ (the last to_date is always without pattern)
    * ~~~~~~~~~~~~~~|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    * Float         | ->Decimal->String->to_date      | ->Decimal->String->to_timestamp->to_utc_timestamp->to_date
    * Decimal       | ->String->to_date               | ->String->->to_timestamp->->to_utc_timestamp->to_date
    * String        | ->to_date                       | ->to_timestamp->->to_utc_timestamp->to_date
    * Timestamp     | ->to_date(no pattern)           | ->to_utc_timestamp->to_date
    * Date          | O                               | ->to_utc_timestamp->to_date
    * Other         | ->String->to_date               | ->String->to_timestamp->to_utc_timestamp->to_date
    */
  private abstract class DateTimeParser[T](implicit defaults: Defaults) extends PrimitiveParser[T] {
    override val field: DateTimeTypeStructField[T]
    protected val pattern: DateTimePattern = field.pattern.get.get

    override protected def assemblePrimitiveCastLogic: Column = {
      if (pattern.isEpoch) {
        castEpoch()
      } else {
        castWithPattern()
      }
    }

    private def patternNeeded(originType: DataType): Unit = {
      if (pattern.isDefault) {
        throw new InvalidParameterException(
          s"Dates & times represented as ${originType.typeName} values need specified 'pattern' metadata"
        )
      }
    }

    private def castWithPattern(): Column = {
      // sadly with parquet support, incoming might not be all `plain`
      origType match {
        case _: DateType                  => castDateColumn(column)
        case _: TimestampType             => castTimestampColumn(column)
        case _: StringType                => castStringColumn(column)
        case ot: DoubleType               =>
          // this case covers some IBM date format where it's represented as a double ddmmyyyy.hhmmss
          patternNeeded(ot)
          castFractionalColumn(column, ot)
        case ot: FloatType                =>
          // this case covers some IBM date format where it's represented as a double ddmmyyyy.hhmmss
          patternNeeded(ot)
          castFractionalColumn(column, ot)
        case ot                           =>
          patternNeeded(ot)
          castNonStringColumn(column, ot)
      }
    }

    private def castFractionalColumn(fractionalColumn: Column, originType: DataType): Column = {
      val index = pattern.indexOf(".") //This can stop working when Spark becomes Locale aware
      val (precision, scale) = if (index == -1) {
        (pattern.length, 0)
      } else {
        (pattern.length-1, pattern.length - index - 1)
      }
      castNonStringColumn(fractionalColumn.cast(DecimalType.apply(precision, scale)), originType)
    }

    private def castNonStringColumn(nonStringColumn: Column, originType: DataType): Column = {
      logger.warn(
        s"$inputFullPathName is specified as timestamp or date, but original type is ${originType.typeName}. Trying to interpret as string."
      )
      castStringColumn(nonStringColumn.cast(StringType))
    }

    protected def castEpoch(): Column = {
      (column.cast(decimalType) / pattern.epochFactor).cast(TimestampType)
    }

    protected def castStringColumn(stringColumn: Column): Column

    protected def castDateColumn(dateColumn: Column): Column

    protected def castTimestampColumn(timestampColumn: Column): Column

  }

  private final case class DateParser(field: DateTimeTypeStructField[Date],
                                      path: String,
                                      origSchema: StructType,
                                      parent: Option[Parent])
                                     (implicit defaults: Defaults) extends DateTimeParser[Date] {
    private val defaultTimeZone: Option[String] = if (pattern.isTimeZoned) {
      pattern.defaultTimeZone
    } else {
      defaults.getDefaultDateTimeZone
    }

    private def applyPatternToStringColumn(column: Column, pattern: String): Column = {
      defaultTimeZone.map(tz =>
        to_date(to_utc_timestamp(to_timestamp(column, pattern), tz))
      ).getOrElse(
        to_date(column, pattern)
      )
    }

    override def castEpoch(): Column = {
      // number cannot be cast to date directly, so first casting to timestamp and then truncating
      to_date(super.castEpoch())
    }

    override protected def castStringColumn(stringColumn: Column): Column = {
      if (pattern.containsSecondFractions) {
        // date doesn't need to care about second fractions
        applyPatternToStringColumn(
          stringColumn.removeSections(
            Seq(pattern.millisecondsPosition, pattern.microsecondsPosition, pattern.nanosecondsPosition).flatten
          ), pattern.patternWithoutSecondFractions)
      } else {
        applyPatternToStringColumn(stringColumn, pattern)
      }
    }

    override protected def castDateColumn(dateColumn: Column): Column = {
      defaultTimeZone.map(
        tz => to_date(to_utc_timestamp(dateColumn, tz))
      ).getOrElse(
        dateColumn
      )
    }

    override protected def castTimestampColumn(timestampColumn: Column): Column = {
      to_date(defaultTimeZone.map(
        to_utc_timestamp(timestampColumn, _)
      ).getOrElse(
        timestampColumn
      ))
    }
  }

  private final case class TimestampParser(field: DateTimeTypeStructField[Timestamp],
                                           path: String,
                                           origSchema: StructType,
                                           parent: Option[Parent])
                                          (implicit defaults: Defaults) extends DateTimeParser[Timestamp] {

    private val defaultTimeZone: Option[String] = if (pattern.isTimeZoned) {
      pattern.defaultTimeZone
    } else {
      defaults.getDefaultTimestampTimeZone
    }

    private def applyPatternToStringColumn(column: Column, pattern: String): Column = {
      val interim: Column = to_timestamp(column, pattern)
      defaultTimeZone.map(to_utc_timestamp(interim, _)).getOrElse(interim)
    }

    override protected def castStringColumn(stringColumn: Column): Column = {
      if (pattern.containsSecondFractions) {
        //this is a trick how to enforce fractions of seconds into the timestamp
        // - turn into timestamp up to seconds precision and that into unix_timestamp,
        // - the second fractions turn into numeric fractions
        // - add both together and convert to timestamp
        val colSeconds = unix_timestamp(applyPatternToStringColumn(
          stringColumn.removeSections(
            Seq(pattern.millisecondsPosition, pattern.microsecondsPosition, pattern.nanosecondsPosition).flatten
          ), pattern.patternWithoutSecondFractions))

        val colMilliseconds: Option[Column] =
          pattern.millisecondsPosition.map(stringColumn.zeroBasedSubstr(_).cast(decimalType) / MillisecondsPerSecond)
        val colMicroseconds: Option[Column] =
          pattern.microsecondsPosition.map(stringColumn.zeroBasedSubstr(_).cast(decimalType) / MicrosecondsPerSecond)
        val colNanoseconds: Option[Column] =
          pattern.nanosecondsPosition.map(stringColumn.zeroBasedSubstr(_).cast(decimalType) / NanosecondsPerSecond)
        val colFractions: Column =
          (colMilliseconds ++ colMicroseconds ++ colNanoseconds).reduceOption(_ + _).getOrElse(lit(0))

        (colSeconds + colFractions).cast(TimestampType)
      } else {
        applyPatternToStringColumn(stringColumn, pattern)
      }
    }

    override protected def castDateColumn(dateColumn: Column): Column = {
      defaultTimeZone.map(
        to_utc_timestamp(dateColumn, _)
      ).getOrElse(
        to_timestamp(dateColumn)
      )
    }

    override protected def castTimestampColumn(timestampColumn: Column): Column = {
      defaultTimeZone.map(
        to_utc_timestamp(timestampColumn, _)
      ).getOrElse(
        timestampColumn
      )
    }
  }
}
