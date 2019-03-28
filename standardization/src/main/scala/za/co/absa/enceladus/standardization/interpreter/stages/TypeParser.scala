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
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.schema.SchemaUtils
import za.co.absa.enceladus.utils.schema.SchemaUtils.appendPath
import za.co.absa.enceladus.utils.types.Defaults
import org.apache.spark.sql.functions._
import za.co.absa.spark.hofs.transform
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.time.DateTimePattern

import scala.util.Random

object TypeParser {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def standardize(field: StructField, path: String, origSchema: StructType)
                 (implicit spark: SparkSession, udfLib: UDFLibrary): ParseOutput = {
    ParserWorker(field, path, origSchema).standardize
  }

  private type Parent = Option[Either[Column, Column]]
  private type ColumnTransformer = Column=>Column

  private object Parent {
    def array(column: Column): Parent = Some(Left(column))
    def struct(column: Column): Parent = Some(Right(column))
  }

  private sealed trait ParserWorker {
    def standardize()(implicit spark: SparkSession, udfLib: UDFLibrary): ParseOutput
    def origSchema: StructType
    def field: StructField
    def path: String
    def parent: Parent
    lazy val fieldName: String = SchemaUtils.getFieldNameOverriddenByMetadata(field)
    lazy val currentAttrPath: String = appendPath(path, fieldName) // absolute path to field to parse
    lazy val column: Column = parent.map {
        case Left(x) => x //parent is array
        case Right(x) => x(fieldName) //parent is struct
      }.getOrElse(col(currentAttrPath)) // no parent
    lazy val isArrayElement: Boolean = parent.exists(_.isLeft)
  }

  private object ParserWorker {
    def apply(field: StructField, path: String, origSchema: StructType, parent: Parent = None): ParserWorker = {
      val workerClass = field.dataType match {
        case _: ArrayType     => ArrayPW
        case _: StructType    => StructPW
        case _: DateType      => DatePW
        case _: TimestampType => TimestampPW
        case _: NumericType   => NumericPW
        case _: StringType    => StringPW
        case _: BooleanType   => BooleanPW
        case t => throw new IllegalStateException(s"${t.typeName} is not a supported type in this version of Enceladus")
      }
      workerClass(field, path, origSchema, parent)
    }

    case class ArrayPW(field: StructField, path: String, origSchema: StructType, parent: Parent) extends ParserWorker {
      private val fieldType: ArrayType = field.dataType.asInstanceOf[ArrayType]
      private val arrayField = StructField(fieldName, fieldType.elementType, fieldType.containsNull)

      def unpath(path: String): String = path.replace('.', '_')

      override def standardize()(implicit spark: SparkSession, udfLib: UDFLibrary): ParseOutput = {
        logger.info(s"Creating standardization plan for Array $currentAttrPath")

        val lambdaVariableName = s"${unpath(currentAttrPath)}_${Random.nextLong().abs}"
        val lambda = (forCol: Column) => ParserWorker(arrayField, path, origSchema, Parent.array(forCol)).standardize

        val lambdaErrCols = lambda.andThen(_.errors)
        val lambdaStdCols = lambda.andThen(_.stdCol)
        val nullErrCond = column.isNull and lit(!field.nullable)

        val finalErrs = when(nullErrCond,
            array(typedLit(ErrorMessage.stdNullErr(currentAttrPath))))
          .otherwise(
            typedLit(flatten(transform(column, lambdaErrCols, lambdaVariableName)))
          )
        val stdCols = transform(column, lambdaStdCols, lambdaVariableName)
        logger.info(s"Finished standardization plan for Array $currentAttrPath")
        ParseOutput(stdCols as fieldName, finalErrs)
      }
    }

    case class StructPW(field: StructField, path: String, origSchema: StructType, parent: Parent) extends ParserWorker {
      private val fieldType: StructType = field.dataType.asInstanceOf[StructType]

      override def standardize()(implicit spark: SparkSession, udfLib: UDFLibrary): ParseOutput = {
        val out =  fieldType.fields.map(ParserWorker(_, currentAttrPath, origSchema, Parent.struct(column)).standardize)
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
            array(callUDF("stdNullErr", lit(currentAttrPath))))
          .otherwise(
            typedLit(Seq[ErrorMessage]())
          )
        )
        // rebuild the struct
        val str = struct(cols: _*).as(field.name)
        ParseOutput(str, errs1)
      }
    }

    trait PrimitivePW extends ParserWorker {
      /** Defines the cast error logic for numeric and other primitive types **/
      //the casting logic differs by class, but within is the same, no need to evalute more timez
      private lazy val primitiveCastLogic: Column = getPrimitiveCastLogic
      protected def getPrimitiveCastLogic: Column

      def primitiveCastErrorLogic: Column = {
        val castedCol = primitiveCastLogic
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

      override def standardize()(implicit spark: SparkSession, udfLib: UDFLibrary): ParseOutput = {
        val err = when((column isNull) and lit(!field.nullable),
          array(callUDF("stdNullErr", lit(s"$currentAttrPath${if (isArrayElement) "[*]" else ""}")))
        ).otherwise(
          when((column isNotNull) and primitiveCastErrorLogic,
            array(
              callUDF("stdCastErr",
                lit(s"$currentAttrPath${if (isArrayElement) "[*]" else ""}"),
                column.cast(StringType))
            )
          ).otherwise(
            typedLit(Seq[ErrorMessage]())
          )
        )

        val std = when((size(err) === lit(0)) and (column isNotNull),
          primitiveCastLogic
        ).otherwise(
          // scalastyle:off null
          when(size(err) === lit(0), null
          // scalastyle:on null
          ).otherwise( Defaults.getDefaultValue(field) )
        ) as field.name

        ParseOutput(std, err)
      }
    }

    trait SimplePW extends  PrimitivePW {
      override def getPrimitiveCastLogic: Column = column.cast(field.dataType)
    }

    case class NumericPW(field: StructField, path: String, origSchema: StructType, parent: Parent) extends SimplePW

    case class StringPW(field: StructField, path: String, origSchema: StructType, parent: Parent) extends SimplePW

    case class BooleanPW(field: StructField, path: String, origSchema: StructType, parent: Parent) extends SimplePW

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
      * Timestamp     | ->to_date                       | ->to_utc_timestamp->to_date
      * Date          | O                               | ->to_utc_timestamp->to_date
      * Other         | ->String                        | ->String->to_timestamp->to_utc_timestamp->to_date
      */

    trait DateTimePW extends PrimitivePW {
      protected def basicCastFunction: (Column, String) => Column  //for epoch casting
      protected def epochPattern: String
      protected val pattern: DateTimePattern = DateTimePattern(field)
      protected lazy val defaultTimeZone: Option[String] = pattern.defaultTimeZone

      private def patternNeeded(originType: DataType): Unit = {
        if (pattern.isDefault) {
          throw new InvalidParameterException(
            s"Dates & times represented as ${originType.typeName} values need specified 'pattern' metadata"
          )
        }
      }

      private def castEpoch(): Column = {
        basicCastFunction(from_unixtime(column.cast("Long")  / pattern.epochFactor, epochPattern), epochPattern)
      }

      private def castWithPattern(): Column = {
        // sadly with parquet support, incoming might not be all `plain`
        val origType: DataType = SchemaUtils.getFieldType(currentAttrPath, origSchema).get
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
        val(precision, scale) = if (index == -1) {
          (pattern.length, 0)
        } else {
          (pattern.length-1, pattern.length - index - 1)
        }
        castNonStringColumn(fractionalColumn.cast(DecimalType.apply(precision, scale)), originType)
      }
      private def castNonStringColumn(nonStringColumn: Column, originType: DataType): Column = {
        logger.warn(
          s"$currentAttrPath is specified as timestamp or date, but original type is ${originType.typeName}. Trying to interpret as string."
        )
        castStringColumn(nonStringColumn.cast(StringType))
      }

      protected def castStringColumn(stringColumn: Column): Column

      protected def castDateColumn(dateColumn: Column): Column

      protected def castTimestampColumn(timestampColumn: Column): Column

      override def getPrimitiveCastLogic: Column = {
        if (DateTimePattern.isEpoch(pattern)) {
          castEpoch()
        } else {
          castWithPattern()
        }
      }
    }

    case class DatePW(field: StructField, path: String, origSchema: StructType, parent: Parent) extends DateTimePW {
      val basicCastFunction: (Column, String) => Column = to_date //for epoch casting
      lazy val epochPattern: String = Defaults.getGlobalFormat(DateType)

      override protected def castStringColumn(stringColumn: Column): Column = {
        defaultTimeZone.map(
          to_utc_timestamp(to_timestamp(stringColumn, pattern), _)
        ).getOrElse(
          to_date(stringColumn, pattern)
        )
      }

      override protected def castDateColumn(dateColumn: Column): Column = {
        defaultTimeZone.map(
          tz =>to_date(to_utc_timestamp(dateColumn, tz))
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

    case class TimestampPW(field: StructField,
                           path: String,
                           origSchema: StructType,
                           parent: Parent) extends DateTimePW {
      val basicCastFunction: (Column, String) => Column = to_timestamp //for epoch casting
      lazy val epochPattern: String = Defaults.getGlobalFormat(TimestampType)

      override protected def castStringColumn(stringColumn: Column): Column = {
        val interim: Column = to_timestamp(stringColumn, pattern)
        defaultTimeZone.map(to_utc_timestamp(interim, _)).getOrElse(interim)
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



}
