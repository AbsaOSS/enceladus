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

import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.schema.SchemaUtils.appendPath
import za.co.absa.enceladus.utils.types.Defaults
import org.apache.spark.sql.functions._
import za.co.absa.spark.hofs.transform
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.time.DateTimePattern

import scala.util.Random

/**
  * Base trait for standardization function
  * Each final class in the hierarchy represents a `standardize` function for its specific data type field
  * Class hierarchy:
  *   TypeParser
  *     ArrayParser !
  *     StructParser !
  *     PrimitiveParser
  *       SimpleParser
  *         NumericParser !
  *         StringParser !
  *         BooleanParser !
  *       DateTimeParser
  *         TimestampParser !
  *         DateParser !
  */
sealed trait TypeParser {
  def standardize(): ParseOutput

  val origSchema: StructType
  val field: StructField
  val path: String
  val parent: Option[TypeParser.Parent]
  val fieldName: String = SchemaUtils.getFieldNameOverriddenByMetadata(field)
  val currentColumnPath: String = appendPath(path, fieldName) // absolute path to field to parse
  val column: Column = parent.map(_.childColumn(fieldName)).getOrElse(col(currentColumnPath)) // no parent
  val isArrayElement: Boolean = parent.exists(_.isInstanceOf[TypeParser.ArrayParent])
}

object TypeParser {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def standardize(field: StructField, path: String, origSchema: StructType): ParseOutput = {
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
                    parent: Option[Parent] = None): TypeParser = {
    val parserClass = field.dataType match {
      case _: ArrayType     => ArrayParser
      case _: StructType    => StructParser
      case _: NumericType   => NumericParser
      case _: StringType    => StringParser
      case _: BooleanType   => BooleanParser
      case _: DateType      => DateParser
      case _: TimestampType => TimestampParser
      case t => throw new IllegalStateException(s"${t.typeName} is not a supported type in this version of Enceladus")
    }
    parserClass(field, path, origSchema, parent)
  }

  private final case class ArrayParser(field: StructField,
                                       path: String,
                                       origSchema: StructType,
                                       parent: Option[Parent]) extends TypeParser {
    private val fieldType = field.dataType.asInstanceOf[ArrayType]
    private val arrayField = StructField(fieldName, fieldType.elementType, fieldType.containsNull)

    def unpath(path: String): String = path.replace('.', '_')

    override def standardize(): ParseOutput = {
      logger.info(s"Creating standardization plan for Array $currentColumnPath")

      val lambdaVariableName = s"${unpath(currentColumnPath)}_${Random.nextLong().abs}"
      val lambda = (forCol: Column) => TypeParser(arrayField, path, origSchema, Option(ArrayParent(forCol)))
        .standardize()

      val lambdaErrCols = lambda.andThen(_.errors)
      val lambdaStdCols = lambda.andThen(_.stdCol)
      val nullErrCond = column.isNull and lit(!field.nullable)

      val finalErrs = when(nullErrCond,
        array(typedLit(ErrorMessage.stdNullErr(currentColumnPath))))
        .otherwise(
          typedLit(flatten(transform(column, lambdaErrCols, lambdaVariableName)))
        )
      val stdCols = transform(column, lambdaStdCols, lambdaVariableName)
      logger.info(s"Finished standardization plan for Array $currentColumnPath")
      ParseOutput(stdCols as fieldName, finalErrs)
    }
  }

  private final case class StructParser(field: StructField,
                                        path: String,
                                        origSchema: StructType,
                                        parent: Option[Parent]) extends TypeParser {
    private val fieldType = field.dataType.asInstanceOf[StructType]

    override def standardize(): ParseOutput = {
      val out =  fieldType.fields.map(TypeParser(_, currentColumnPath, origSchema, Option(StructParent(column)))
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
          array(callUDF("stdNullErr", lit(currentColumnPath))))
          .otherwise(
            typedLit(Seq[ErrorMessage]())
          )
      )
      // rebuild the struct
      val str = struct(cols: _*).as(field.name)
      ParseOutput(str, errs1)
    }
  }

  private trait PrimitiveParser extends TypeParser {
    override def standardize(): ParseOutput = {
      val castedCol: Column = primitiveCastLogic
      val err = when((column isNull) and lit(!field.nullable),
        array(callUDF("stdNullErr", lit(s"$currentColumnPath${if (isArrayElement) "[*]" else ""}")))
      ).otherwise(
        when((column isNotNull) and primitiveCastErrorLogic(castedCol),
          array(
            callUDF("stdCastErr",
              lit(s"$currentColumnPath${if (isArrayElement) "[*]" else ""}"),
              column.cast(StringType))
          )
        ).otherwise(
          typedLit(Seq[ErrorMessage]())
        )
      )

      val std = when((size(err) === lit(0)) and (column isNotNull),
        castedCol
      ).otherwise(
        // scalastyle:off null
        when(size(err) === lit(0), null
          // scalastyle:on null
        ).otherwise( Defaults.getDefaultValue(field) )
      ) as field.name

      ParseOutput(std, err)
    }

    protected def primitiveCastLogic: Column //this differs based on the field data type

    private def primitiveCastErrorLogic(castedCol: Column): Column = { //this one is same for all primitive data types
      field.dataType match {
        // here we also want to check the numeric overflow by comparing with the original value this could break with
        // trailing or leading zeros
        case _: IntegerType |
             _: ShortType |
             _: FloatType |
             _: DoubleType |
             _: DecimalType |
             _: ByteType |
             _: LongType => castedCol isNull //TODO actual overflow/underflow check (#251)
        case _ => castedCol isNull
      }
    }

  }

  private trait SimpleParser extends  PrimitiveParser {
    override def primitiveCastLogic: Column = column.cast(field.dataType)
  }

  private final case class NumericParser(field: StructField,
                                         path: String,
                                         origSchema: StructType,
                                         parent: Option[Parent]) extends SimpleParser

  private final case class StringParser(field: StructField,
                                        path: String,
                                        origSchema: StructType,
                                        parent: Option[Parent]) extends SimpleParser

  private final case class BooleanParser(field: StructField,
                                         path: String,
                                         origSchema: StructType,
                                         parent: Option[Parent]) extends SimpleParser

  /**
    * Timestamp conversion logic
    * Original type | TZ in pattern/without TZ        | Has default TZ
    * ==============|=================================|============================================================
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
    * ==============|=================================|============================================================
    * Float         | ->Decimal->String->to_date      | ->Decimal->String->to_timestamp->to_utc_timestamp->to_date
    * Decimal       | ->String->to_date               | ->String->->to_timestamp->->to_utc_timestamp->to_date
    * String        | ->to_date                       | ->to_timestamp->->to_utc_timestamp->to_date
    * Timestamp     | ->to_date(no pattern)           | ->to_utc_timestamp->to_date
    * Date          | O                               | ->to_utc_timestamp->to_date
    * Other         | ->String->to_date               | ->String->to_timestamp->to_utc_timestamp->to_date
    */
  private trait DateTimeParser extends PrimitiveParser {
    protected val basicCastFunction: (Column, String) => Column  //for epoch casting
    protected val pattern: DateTimePattern = DateTimePattern.fromStructField(field)

    override protected def primitiveCastLogic: Column = {
      if (DateTimePattern.isEpoch(pattern)) {
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

    private def castEpoch(): Column = {
      val epochPattern: String = Defaults.getGlobalFormat(field.dataType)
      basicCastFunction(from_unixtime(column.cast("Long")  / pattern.epochFactor, epochPattern), epochPattern)
    }

    private def castWithPattern(): Column = {
      // sadly with parquet support, incoming might not be all `plain`
      val origType: DataType = SchemaUtils.getFieldType(currentColumnPath, origSchema).get
      origType match {
        case _: DateType                  => castDateColumn(column)
        case _: TimestampType             => castTimestampColumn(column)
        case _: StringType                => castStringColumn(column)
        case _: DoubleType | _: FloatType =>
          // this case covers some IBM date format where it's represented as a double ddmmyyyy.hhmmss
          patternNeeded(origType)
          castFractionalColumn(column, origType)
        case _                            =>
          patternNeeded(origType)
          castNonStringColumn(column, origType)
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
        s"$currentColumnPath is specified as timestamp or date, but original type is ${originType.typeName}. Trying to interpret as string."
      )
      castStringColumn(nonStringColumn.cast(StringType))
    }

    protected def castStringColumn(stringColumn: Column): Column

    protected def castDateColumn(dateColumn: Column): Column

    protected def castTimestampColumn(timestampColumn: Column): Column

  }

  private final case class DateParser(field: StructField,
                                      path: String,
                                      origSchema: StructType,
                                      parent: Option[Parent]) extends DateTimeParser {
    protected val basicCastFunction: (Column, String) => Column = to_date //for epoch casting

    override protected def castStringColumn(stringColumn: Column): Column = {
      pattern.defaultTimeZone.map(tz =>
        to_date(to_utc_timestamp(to_timestamp(stringColumn, pattern), tz))
      ).getOrElse(
        to_date(stringColumn, pattern)
      )
    }

    override protected def castDateColumn(dateColumn: Column): Column = {
      pattern.defaultTimeZone.map(
        tz => to_date(to_utc_timestamp(dateColumn, tz))
      ).getOrElse(
        dateColumn
      )
    }

    override protected def castTimestampColumn(timestampColumn: Column): Column = {
      to_date(pattern.defaultTimeZone.map(
        to_utc_timestamp(timestampColumn, _)
      ).getOrElse(
        timestampColumn
      ))
    }
  }

  private final case class TimestampParser(field: StructField,
                                           path: String,
                                           origSchema: StructType,
                                           parent: Option[Parent]) extends DateTimeParser {
    protected val basicCastFunction: (Column, String) => Column = to_timestamp //for epoch casting

    override protected def castStringColumn(stringColumn: Column): Column = {
      val interim: Column = to_timestamp(stringColumn, pattern)
      pattern.defaultTimeZone.map(to_utc_timestamp(interim, _)).getOrElse(interim)
    }

    override protected def castDateColumn(dateColumn: Column): Column = {
      pattern.defaultTimeZone.map(
        to_utc_timestamp(dateColumn, _)
      ).getOrElse(
        to_timestamp(dateColumn)
      )
    }

    override protected def castTimestampColumn(timestampColumn: Column): Column = {
      pattern.defaultTimeZone.map(
        to_utc_timestamp(timestampColumn, _)
      ).getOrElse(
        timestampColumn
      )
    }
  }
}
