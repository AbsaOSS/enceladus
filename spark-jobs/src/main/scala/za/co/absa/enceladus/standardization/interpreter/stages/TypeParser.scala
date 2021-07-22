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

package za.co.absa.enceladus.standardization.interpreter.stages

import java.security.InvalidParameterException
import java.sql.Timestamp
import java.util.Date
import java.util.regex.Pattern

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.schema.SchemaUtils.FieldWithSource
import za.co.absa.enceladus.utils.schema.{MetadataValues, SchemaUtils}
import za.co.absa.enceladus.utils.time.DateTimePattern
import za.co.absa.enceladus.utils.typeClasses.{DoubleLike, LongLike}
import za.co.absa.enceladus.utils.types.TypedStructField._
import za.co.absa.enceladus.utils.types.{Defaults, TypedStructField}
import za.co.absa.enceladus.utils.udf.{UDFBuilder, UDFLibrary, UDFNames}
import za.co.absa.spark.hofs.transform

import scala.reflect.runtime.universe._
import scala.util.{Random, Try}

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
  *       BinaryParser !
  *       DateTimeParser
  *         TimestampParser !
  *         DateParser !
  */
sealed trait TypeParser[T] {

  def standardize()(implicit logger: Logger): ParseOutput = {
    checkSetupForFailure().getOrElse(
      standardizeAfterCheck()
    )
  }

  protected val failOnInputNotPerSchema: Boolean
  protected val field: TypedStructField
  protected val metadata: Metadata = field.structField.metadata
  protected val path: String
  protected val origType: DataType
  protected val fieldInputName: String = field.structField.sourceName
  protected val fieldOutputName: String = field.name
  protected val inputFullPathName: String = SchemaUtils.appendPath(path, fieldInputName)
  protected val isArrayElement: Boolean
  protected val columnIdForUdf: String = if (isArrayElement) {
      s"$inputFullPathName[*]"
    } else {
      inputFullPathName
    }

  protected val column: Column

  protected def fieldType: DataType = field.dataType

  // Error should never appear here due to validation
  protected def defaultValue: Option[field.BaseType] = field.defaultValueWithGlobal.get

  protected def checkSetupForFailure()(implicit logger: Logger): Option[ParseOutput] = {
    def noCastingPossible: Option[ParseOutput] = {
      val message = s"Cannot standardize field '$inputFullPathName' from type ${origType.typeName} into ${fieldType.typeName}"
      if (failOnInputNotPerSchema) {
        throw new TypeParserException(message)
      } else {
        logger.info(message)
        Option(ParseOutput(
          lit(defaultValue.orNull).cast(fieldType) as(fieldOutputName, metadata),
          typedLit(Seq(ErrorMessage.stdTypeError(inputFullPathName, origType.typeName, fieldType.typeName)))
        ))
      }
    }

    (origType, fieldType) match {
      case (ArrayType(_, _), ArrayType(_, _)) => None
      case (StructType(_), StructType(_)) => None
      case (ArrayType(_, _), _) => noCastingPossible
      case (_, ArrayType(_, _)) => noCastingPossible
      case (StructType(_), _) => noCastingPossible
      case (_, StructType(_)) => noCastingPossible
      case _ => None
    }
  }

  protected def standardizeAfterCheck()(implicit logger: Logger): ParseOutput
}

object TypeParser {
  import za.co.absa.enceladus.utils.implicits.ColumnImplicits.ColumnEnhancements

  private val decimalType = DecimalType(30,9) // scalastyle:ignore magic.number
  private implicit val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val MillisecondsPerSecond = 1000
  private val MicrosecondsPerSecond = 1000000
  private val NanosecondsPerSecond  = 1000000000
  private val InfinityStr = "Infinity"
  private val nullColumn = lit(null) //scalastyle:ignore null


  def standardize(field: StructField, path: String, origSchema: StructType, failOnInputNotPerSchema: Boolean = true)
                 (implicit udfLib: UDFLibrary, defaults: Defaults): ParseOutput = {
    // udfLib implicit is present for error column UDF implementation
    val sourceName = SchemaUtils.appendPath(path, field.sourceName)
    val origField = SchemaUtils.getField(sourceName, origSchema)
    val origFieldType = origField.map(_.dataType).getOrElse(NullType)
    val column = origField.fold(nullColumn)(_ => col(sourceName))
    TypeParser(field, path, column, origFieldType, failOnInputNotPerSchema).standardize()
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
                    column: Column,
                    origType: DataType,
                    failOnInputNotPerSchema: Boolean,
                    isArrayElement: Boolean = false)
                   (implicit defaults: Defaults): TypeParser[_] = {
    val parserClass: (String, Column, DataType, Boolean, Boolean) => TypeParser[_] = field.dataType match {
      case _: ArrayType     => ArrayParser(TypedStructField.asArrayTypeStructField(field), _, _, _, _, _)
      case _: StructType    => StructParser(TypedStructField.asStructTypeStructField(field), _, _, _, _, _)
      case _: ByteType      =>
        IntegralParser(TypedStructField.asNumericTypeStructField[Byte](field), _, _, _, _, _, Set(ShortType, IntegerType, LongType))
      case _: ShortType     =>
        IntegralParser(TypedStructField.asNumericTypeStructField[Short](field), _, _, _, _, _, Set(IntegerType, LongType))
      case _: IntegerType   => IntegralParser(TypedStructField.asNumericTypeStructField[Int](field), _, _, _, _, _, Set(LongType))
      case _: LongType      => IntegralParser(TypedStructField.asNumericTypeStructField[Long](field), _, _, _, _, _, Set.empty)
      case _: FloatType     => FractionalParser(TypedStructField.asNumericTypeStructField[Float](field), _, _, _, _, _)
      case _: DoubleType    => FractionalParser(TypedStructField.asNumericTypeStructField[Double](field), _, _, _, _, _)
      case _: DecimalType   => DecimalParser(TypedStructField.asNumericTypeStructField[BigDecimal](field), _, _, _, _, _)
      case _: StringType    => StringParser(TypedStructField(field), _, _, _, _, _)
      case _: BinaryType    => BinaryParser(TypedStructField.asBinaryTypeStructField(field), _, _, _, _, _)
      case _: BooleanType   => BooleanParser(TypedStructField(field), _, _, _, _, _)
      case _: DateType      => DateParser(TypedStructField.asDateTimeTypeStructField(field), _, _, _, _, _)
      case _: TimestampType => TimestampParser(TypedStructField.asDateTimeTypeStructField(field), _, _, _, _, _)
      case t                => throw new IllegalStateException(s"${t.typeName} is not a supported type in this version of Enceladus")
    }
    parserClass(path, column, origType, failOnInputNotPerSchema, isArrayElement)
  }

  private final case class ArrayParser(override val field: ArrayTypeStructField,
                                       path: String,
                                       column: Column,
                                       origType: DataType,
                                       failOnInputNotPerSchema: Boolean,
                                       isArrayElement: Boolean)
                                      (implicit defaults: Defaults) extends TypeParser[Any] {

    override def fieldType: ArrayType = {
      field.dataType
    }

    override protected def standardizeAfterCheck()(implicit logger: Logger): ParseOutput = {
      logger.info(s"Creating standardization plan for Array $inputFullPathName")
      val origArrayType = origType.asInstanceOf[ArrayType] // this should never throw an exception because of `checkSetupForFailure`
      val arrayField = StructField(fieldInputName, fieldType.elementType, fieldType.containsNull, field.structField.metadata)
      val lambdaVariableName = s"${SchemaUtils.unpath(inputFullPathName)}_${Random.nextLong().abs}"
      val lambda = (forCol: Column) => TypeParser(arrayField, path, forCol, origArrayType.elementType, failOnInputNotPerSchema, isArrayElement = true)
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
                                        column: Column,
                                        origType: DataType,
                                        failOnInputNotPerSchema: Boolean,
                                        isArrayElement: Boolean)
                                       (implicit defaults: Defaults) extends TypeParser[Any] {
    override def fieldType: StructType = {
      field.dataType
    }

    override protected def standardizeAfterCheck()(implicit logger: Logger): ParseOutput = {
      val origStructType = origType.asInstanceOf[StructType] // this should never throw an exception because of `checkSetupForFailure`
      val out =  fieldType.fields.map{f =>
        val origSubField = Try{origStructType(f.sourceName)}.toOption
        val origSubFieldType = origSubField.map(_.dataType).getOrElse(NullType)
        val subColumn = origSubField.map(x => column(x.name)).getOrElse(nullColumn)
        TypeParser(f, inputFullPathName, subColumn, origSubFieldType, failOnInputNotPerSchema).standardize()}
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
          array(callUDF(UDFNames.stdNullErr, lit(inputFullPathName))))
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
    override protected def standardizeAfterCheck()(implicit logger: Logger): ParseOutput = {
      val castedCol: Column = assemblePrimitiveCastLogic
      val castHasError: Column = assemblePrimitiveCastErrorLogic(castedCol)

      val err: Column  = if (field.nullable) {
        when(column.isNotNull and castHasError, // cast failed
          array(callUDF(UDFNames.stdCastErr, lit(columnIdForUdf), column.cast(StringType)))
        ).otherwise( // everything is OK
          typedLit(Seq.empty[ErrorMessage])
        )
      } else {
        when(column.isNull, // NULL not allowed
          array(callUDF(UDFNames.stdNullErr, lit(columnIdForUdf)))
        ).otherwise( when(castHasError, // cast failed
          array(callUDF(UDFNames.stdCastErr, lit(columnIdForUdf), column.cast(StringType)))
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
  }

  private abstract class ScalarParser[T](implicit defaults: Defaults) extends PrimitiveParser[T] {
    override def assemblePrimitiveCastLogic: Column = column.cast(field.dataType)
  }

  private abstract class NumericParser[N: TypeTag](override val field: NumericTypeStructField[N])
                                                  (implicit defaults: Defaults) extends ScalarParser[N] {
    override protected def standardizeAfterCheck()(implicit logger: Logger): ParseOutput = {
      if (field.needsUdfParsing) {
        standardizeUsingUdf()
      } else {
        super.standardizeAfterCheck()
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
                                                                column: Column,
                                                                origType: DataType,
                                                                failOnInputNotPerSchema: Boolean,
                                                                isArrayElement: Boolean,
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
                                         column: Column,
                                         origType: DataType,
                                         failOnInputNotPerSchema: Boolean,
                                         isArrayElement: Boolean)
                                        (implicit defaults: Defaults)
    extends NumericParser[BigDecimal](field)
    // NB! loss of precision is not addressed for any DecimalType
    // e.g. 3.141592 will be Standardized to Decimal(10,2) as 3.14

  private final case class FractionalParser[N: DoubleLike: TypeTag](override val field: NumericTypeStructField[N],
                                                                    path: String,
                                                                    column: Column,
                                                                    origType: DataType,
                                                                    failOnInputNotPerSchema: Boolean,
                                                                    isArrayElement: Boolean)
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
                                        column: Column,
                                        origType: DataType,
                                        failOnInputNotPerSchema: Boolean,
                                        isArrayElement: Boolean)
                                       (implicit defaults: Defaults) extends ScalarParser[String]

  private final case class BinaryParser(field: BinaryTypeStructField,
                                        path: String,
                                        column: Column,
                                        origType: DataType,
                                        failOnInputNotPerSchema: Boolean,
                                        isArrayElement: Boolean)
                                       (implicit defaults: Defaults) extends PrimitiveParser[Array[Byte]] {
    override protected def assemblePrimitiveCastLogic: Column = {
      origType match {
        case BinaryType => column
        case StringType =>
          // already validated in Standardization
          field.normalizedEncoding match {
            case Some(MetadataValues.Encoding.Base64) => callUDF(UDFNames.binaryUnbase64, column)
            case Some(MetadataValues.Encoding.None) | None =>
              if (field.normalizedEncoding.isEmpty) {
                logger.warn(s"Binary field ${field.structField.name} does not have encoding setup in metadata. Reading as-is.")
              }
              column.cast(field.dataType) // use as-is
            case _ => throw new IllegalStateException(s"Unsupported encoding for Binary field ${field.structField.name}:" +
              s" '${field.normalizedEncoding.get}'")
          }

        case _ => throw new IllegalStateException(s"Unsupported conversion from BinaryType to ${field.dataType}")
      }
    }
  }

  private final case class BooleanParser(field: TypedStructField,
                                         path: String,
                                         column: Column,
                                         origType: DataType,
                                         failOnInputNotPerSchema: Boolean,
                                         isArrayElement: Boolean)
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
//      underlyingType match {
      origType match {
        case _: NullType                  => nullColumn
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
                                      column: Column,
                                      origType: DataType,
                                      failOnInputNotPerSchema: Boolean,
                                      isArrayElement: Boolean)
                                     (implicit defaults: Defaults) extends DateTimeParser[Date] {
    private val defaultTimeZone: Option[String] = field.defaultTimeZone.map(Option(_)).getOrElse(defaults.getDefaultDateTimeZone)

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
                                           column: Column,
                                           origType: DataType,
                                           failOnInputNotPerSchema: Boolean,
                                           isArrayElement: Boolean)
                                          (implicit defaults: Defaults) extends DateTimeParser[Timestamp] {

    private val defaultTimeZone: Option[String] = field.defaultTimeZone.map(Option(_)).getOrElse(defaults.getDefaultTimestampTimeZone)

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

class TypeParserException(message: String) extends Exception(message: String)
