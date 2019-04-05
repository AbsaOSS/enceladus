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

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import za.co.absa.enceladus.utils.types.Defaults
import scala.util.Random
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.schema.SchemaUtils.appendPath
import za.co.absa.spark.hofs._
import scala.collection.Seq
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.schema.SchemaUtils
import org.slf4j.{Logger, LoggerFactory}
import java.security.InvalidParameterException
import za.co.absa.enceladus.utils.time.DateTimePattern

object TypeParser {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val epochTimestampFormat: String = Defaults.getGlobalFormat(TimestampType)
  private val epochDateFormat: String = Defaults.getGlobalFormat(DateType)
  private type colStrToColFnc = (Column, String) => Column

  /** Defines the Spark SQL (DSL) logic for parsing different (primitive) types of columns  */
  private def primitiveCastLogic(field: StructField, origSchema: StructType, path: String, attr: Column): Column = {
    field.dataType match {
      case _: TimestampType => toTimestamp(field, origSchema, path, attr)
      case _: DateType      => toDate(field, origSchema, path, attr)
      case t: DataType      => attr.cast(t)
      case t                =>
        throw new IllegalStateException(
          s"${t.typeName} type cannot be cast to a strong typed format in this version of Enceladus"
        )
    }
  }

  /**
    * Deals with specific weirdness that we see with dates & times coming from various tech stacks
    * Also supports epoch format
    */
  private def dateTimePreProcess(path: String,
                                 attr: Column,
                                 pattern: DateTimePattern,
                                 origType: DataType,
                                 castFunction: colStrToColFnc): Column = {
    val sourceCol = origType match {
      case _: DoubleType | _: FloatType =>
        logger.warn(s"$path is specified as timestamp or date, but original type is ${origType.typeName}. " +
          "Trying to interpret as string.")
        if (pattern.isDefault) {
          throw new InvalidParameterException(
            s"Dates & times represented as ${origType.typeName} values need specified 'pattern' metadata"
          )
        }
        val index = pattern.indexOf(".") //This can stop working when Spark becomes Locale aware
        val (precision, scale) = if (index == -1) {
          (pattern.length, 0)
        } else {
          (pattern.length-1, pattern.length - index - 1)
        }
        attr.cast(DecimalType.apply(precision, scale)).cast(StringType)
      case _: StringType => attr
      case _ =>
        logger.warn(s"$path is specified as timestamp or date, but original type is ${origType.typeName}. " +
          "Trying to interpret as string.")
        if (pattern.isDefault) {
          throw new InvalidParameterException(
            s"Dates & times represented as ${origType.typeName} values need specified 'pattern' metadata"
          )
        }
        attr.cast(StringType)
    }
    castFunction(sourceCol, pattern)
  }

  private def castEpoch(attr: Column, epochFactor: Long, epochPattern: String, castFunction: colStrToColFnc): Column =
    castFunction(from_unixtime(attr.cast("Long")  / epochFactor, epochPattern), epochPattern)

  private def toTimestampWithDefaultTimeZone(col: Column, pattern: String, defaultTimeZone: String): Column =
    to_utc_timestamp(to_timestamp(col, pattern), defaultTimeZone)

  private def toDateWithDefaultTimeZone(col: Column, pattern: String, defaultTimeZone: String): Column =
    to_date(to_utc_timestamp(to_timestamp(col, pattern), defaultTimeZone))

  private def toTimestamp(field: StructField, origSchema: StructType, path: String, attr: Column): Column = {
    val pattern = DateTimePattern.fromStructField(field)
    if (pattern.isEpoch) {
     castEpoch(attr, pattern.epochFactor, epochTimestampFormat, to_timestamp)
    } else {
      // sadly with parquet support, incoming might not be all `plain`
      val origType: DataType = SchemaUtils.getFieldType(path, origSchema).get
      (origType, pattern.defaultTimeZone) match {
        case (TimestampType, None) => attr
        case (DateType, None) => to_timestamp(attr)
        case (TimestampType, Some(tz)) => to_utc_timestamp(attr, tz)
        case (DateType, Some(tz)) => to_utc_timestamp(attr, tz)
        case (_, Some(tz)) =>
          dateTimePreProcess(path, attr, pattern, origType, toTimestampWithDefaultTimeZone(_, _, tz))
        case _ => dateTimePreProcess(path, attr, pattern, origType, to_timestamp)
      }
    }
  }

  private def toDate(field: StructField, origSchema: StructType, path: String, attr: Column): Column = {
    val pattern = DateTimePattern.fromStructField(field)
    if (pattern.isEpoch) {
      castEpoch(attr, pattern.epochFactor, epochDateFormat, to_date)
    } else {
      // sadly with parquet support, incoming might not be all `plain`
      val origType: DataType = SchemaUtils.getFieldType(path, origSchema).get
      (origType, pattern.defaultTimeZone) match {
        case (TimestampType, None) => to_date(attr)
        case (DateType, None) => attr
        case (TimestampType, Some(tz)) => to_date(to_utc_timestamp(attr, tz))
        case (DateType, Some(tz)) => to_date(to_utc_timestamp(attr, tz))
        case (_, Some(tz)) => dateTimePreProcess(path, attr, pattern, origType, toDateWithDefaultTimeZone(_, _, tz))
        case _ => dateTimePreProcess(path, attr, pattern, origType, to_date)
      }
    }
  }

  /** Defines the cast error logic for numeric and other primitive types **/
  private def primitiveCastErrorLogic(field: StructField,
                                      origSchema: StructType,
                                      path: String,
                                      attr: Column): Column = {
    val castedCol = primitiveCastLogic(field, origSchema, path, attr)
    field.dataType match {
      // here we also want to check the numeric overflow by comparing with the original value this could break with
      // trailing or leading zeros
      case _: IntegerType |
           _: ShortType |
           _: FloatType |
           _: DoubleType |
           _: DecimalType |
           _: ByteType |
           _: LongType => castedCol isNull
      case _ => castedCol isNull
    }
  }

  /** Remove dots from paths to be used as flat column names **/
  private def unpath(path: String): String = path.replace('.', '_')

  private def standardizeArrayField(field: StructField,
                                    path: String,
                                    origSchema: StructType,
                                    attr: Column,
                                    fieldName: String,
                                    currentAttrPath: String,
                                    fieldType: ArrayType): ParseOutput = {
    logger.info(s"Creating standardization plan for Array $currentAttrPath")

    val newField = StructField(name = fieldName, dataType = fieldType.elementType, nullable = fieldType.containsNull)
    val lambdaVariableName = s"${unpath(currentAttrPath)}_${Random.nextInt().abs}"
    //here pass lambda (current element)
    val lambda = (i: Column) => standardize(newField, path, origSchema, Some(i), isArrayElement = true)
    val lambdaErrCols = lambda.andThen(_.errors)
    val lambdaStdCols = lambda.andThen(_.stdCol)

    val nullErrCond = attr.isNull and lit(!field.nullable)

    val finalErrs = when(nullErrCond,
      array(typedLit(ErrorMessage.stdNullErr(currentAttrPath)))
    ).otherwise(
      typedLit(flatten(transform(attr, lambdaErrCols, lambdaVariableName)))
    )
    val stdCols = transform(attr, lambdaStdCols, lambdaVariableName)
    logger.info(s"Finished standardization plan for Array $currentAttrPath")
    ParseOutput(stdCols as fieldName, finalErrs)
  }

  private def standardizeStructField(field: StructField,
                                     origSchema: StructType,
                                     attr: Column,
                                     currentAttrPath: String,
                                     fieldType: StructType): ParseOutput = {
    // conform all children.. this has to be foldleft - children could be arrays modifying the DF, therefore needs to be
    // chained
    val stdOut =  fieldType.fields.map(standardize(_, currentAttrPath, origSchema, Some(attr)))
    val cols = stdOut.map(_.stdCol)
    val errs = stdOut.map(_.errors)
    // condition for nullable error of the struct itself
    val nullErrCond = attr.isNull and lit(!field.nullable)
    val dropChildrenErrsCond = attr.isNull
    // first remove all child errors if this is null
    val errs1 = concat(
      flatten(array(errs.map(x => when(dropChildrenErrsCond, typedLit(Seq[ErrorMessage]())).otherwise(x)): _*)),
      // then add an error if this one is null
      when(nullErrCond, array(callUDF("stdNullErr", lit(currentAttrPath)))).otherwise(typedLit(Seq[ErrorMessage]())))
    // rebuild the struct
    val str = struct(cols: _*).as(field.name)
    ParseOutput(str, errs1)
  }

  private def standardizeStandardField(field: StructField,
                                       origSchema: StructType,
                                       isArrayElement: Boolean,
                                       attr: Column, currentAttrPath: String): ParseOutput = {
    val default = Defaults.getDefaultValue(field)

    val err = when((attr isNull) and lit(!field.nullable),
      array(callUDF("stdNullErr", lit(s"$currentAttrPath${if (isArrayElement) "[*]" else ""}")))
    ).otherwise(
      when((attr isNotNull) and primitiveCastErrorLogic(field, origSchema, currentAttrPath, attr),
        array(
          callUDF("stdCastErr", lit(s"$currentAttrPath${if (isArrayElement) "[*]" else ""}"),
          attr.cast(StringType))
        )
      ).otherwise(
        typedLit(Seq[ErrorMessage]())
      )
    )

    val std = when((size(err) === lit(0)) and (attr isNotNull),
      primitiveCastLogic(field, origSchema, currentAttrPath, attr)
    ).otherwise(
      // scalastyle:off null
      when(size(err) === lit(0), null
      // scalastyle:on null
      ).otherwise(default)
    ) as field.name

    ParseOutput(std, err)
  }

  def standardize(field: StructField,
                  path: String,
                  origSchema: StructType,
                  parent: Option[Column] = None,
                  isArrayElement: Boolean = false): ParseOutput = {
    // If the meta data value sourcecolumn is set to override the field name
    val fieldName = SchemaUtils.getFieldNameOverriddenByMetadata(field)

    //import spark.implicits._
    val currentAttrPath = appendPath(path, fieldName) // calculate the absolute path to this attribute
    val thisCol =
      if (isArrayElement) parent.get else if (parent.isDefined) parent.get(fieldName) else col(currentAttrPath)

    field.dataType match {
      case a: ArrayType => standardizeArrayField(field, path, origSchema, thisCol, fieldName, currentAttrPath, a)
      case t: StructType => standardizeStructField(field, origSchema, thisCol, currentAttrPath, t)
      case _: NumericType | _: StringType | _: BooleanType | _: DateType | _: TimestampType =>
        standardizeStandardField(field, origSchema, isArrayElement, thisCol, currentAttrPath)
      case t => throw new IllegalStateException(s"${t.typeName} is not a supported type in this version of Enceladus")
    }
  }

}
