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

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import za.co.absa.enceladus.utils.types.Defaults
import scala.util.Random
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.schema.SchemaUtils.appendPath
import za.co.absa.enceladus.utils.error.UDFLibrary
import scala.collection.Seq
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.schema.SchemaUtils
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException

object TypeParser {

  val logger = LoggerFactory.getLogger(this.getClass)

  /** Defines the Spark SQL (DSL) logic for parsing different (primitive) types of columns  */
  private def primitiveCastLogic(field: StructField, origSchema: StructType, path: String, attr: Column): Column = {
    field.dataType match {
      case _: TimestampType => to_timestamp(dateTimePreProcess(field, origSchema, path, attr), Defaults.getFormat(field))
      case _: DateType      => to_date(dateTimePreProcess(field, origSchema, path, attr), Defaults.getFormat(field))
      case t: DataType      => attr.cast(t)
      case t                => throw new IllegalStateException(s"${t.typeName} type cannot be cast to a strong typed format in this version of Enceladus")
    }
  }

  /** Deals with specific weirdness that we see with dates & times coming from various tech stacks */
  private def dateTimePreProcess(field: StructField, origSchema: StructType, path: String, attr: Column): Column = {
    val patternOpt = Defaults.getFormatOpt(field)
    val patternGlobal = Defaults.getFormat(field)
    val origType = SchemaUtils.getFieldType(path, origSchema) // sadly with parquet support, incoming might not be all `plain`

    // this case covers some IBM date format where it's represented as a double ddmmyyyy.hhmmss
    origType.get match {
      case DoubleType => {
        logger.warn(s"${path} is specified as timestamp or date, but original type is Double. Trying to interpret as string.")
        if (patternOpt.isEmpty) throw new InvalidParameterException("Dates & times represented as numeric values need specified 'pattern' metadata")
        val Array(l, r) = patternGlobal.replaceAll("'", "").replace("\"", "").split("\\.")
        val precision = l.length() + r.length()
        val scale = r.length()

        attr.cast(DecimalType.apply(precision, scale)).cast(StringType)
      }
      case t: DecimalType => {
        logger.warn(s"${path} is specified as timestamp or date, but original type is Decimal. Trying to interpret as string.")
        if (patternOpt.isEmpty) throw new InvalidParameterException("Dates & times represented as numeric values need specified 'pattern' metadata")
        attr.cast(StringType)
      }
      case _ => attr
    }
  }

  /** Defines the cast error logic for numeric and other primitive types **/
  private def primitiveCastErrorLogic(field: StructField, origSchema: StructType, path: String, attr: Column): Column = {
    val castedCol = primitiveCastLogic(field, origSchema, path, attr)
    field.dataType match {
      // here we also want to check the numeric overflow by comparing with the original value this could break with trailing or leading zeros
      case _: IntegerType | _: ShortType | _: FloatType | _: DoubleType | _: DecimalType | _: ByteType | _: LongType => (castedCol isNull)
      case _ => castedCol isNull
    }
  }

  /** Remove dots from paths to be used as flat column names **/
  private def unpath(path: String): String = path.replace('.', '_')

  def standardize(field: StructField, path: String, origSchema: StructType, parent: Option[Column] = None, isArrayElement: Boolean = false)(implicit spark: SparkSession, udfLib: UDFLibrary): ParseOutput = {
    // If the meta data value sourcecolumn is set to override the field name
    val fieldName = SchemaUtils.getFieldNameOverriddenByMetadata(field)

    import spark.implicits._
    val currentAttrPath = appendPath(path, fieldName) // calculate the absolute path to this attribute
    val thisCol = if (isArrayElement) parent.get else if (parent.isDefined) parent.get(fieldName) else col(currentAttrPath)
    val errField = unpath(currentAttrPath) + Random.nextLong().abs

    field.dataType match {
      case a @ ArrayType(elType, nullable) => {

        logger.info(s"Creating standardization plan for Array ${currentAttrPath}")

        val newfield = StructField(name = fieldName, dataType = elType, nullable = a.containsNull)

        val lambdaName = s"${unpath(currentAttrPath)}_${Random.nextInt().abs}"

        val ParseOutput(stdCol, errCols) = standardize(newfield, path, origSchema, Some(_$(lambdaName)), isArrayElement = true) // here pass lambda (current element)

        val nullErrCond = (thisCol.isNull) and lit(!field.nullable)

        val finalErrs = when(nullErrCond, array(typedLit(ErrorMessage.stdNullErr(currentAttrPath)))).otherwise(typedLit(flatten(transform(thisCol, lambdaName, errCols))))
        val stdCols = transform(thisCol, lambdaName, stdCol)

        logger.info(s"Finished standardization plan for Array ${currentAttrPath}")

        ParseOutput(stdCols as fieldName, finalErrs)
      }

      case t: StructType => {
        // conform all children.. this has to be foldleft - children could be arrays modifying the DF, therefore needs to be chained
        val stdOut = t.fields.map(x => standardize(x, currentAttrPath, origSchema, Some(thisCol)))

        val cols = stdOut.map(_.stdCol)
        val errs = stdOut.map(_.errors)

        // condition for nullable error of the struct itself
        val nullErrCond = thisCol.isNull and lit(!field.nullable)
        val dropChildrenErrsCond = thisCol.isNull

        // first remove all child errors if this is null
        val errs1 = concat(
          flatten(array(errs.map(x => when(dropChildrenErrsCond, typedLit(Seq[ErrorMessage]())).otherwise(x)): _*)),
          // then add an error if this one is null
          when(nullErrCond, array(callUDF("stdNullErr", lit(currentAttrPath)))).otherwise(typedLit(Seq[ErrorMessage]())))

        // rebuild the struct
        val str = struct(cols: _*).as(field.name)
        ParseOutput(str, errs1)

      }

      case _: NumericType | _: StringType | _: BooleanType | _: DateType | _: TimestampType => {

        val default = Defaults.getDefaultValue(field)

        val err = when((thisCol isNull) and lit(!field.nullable), array(callUDF("stdNullErr", lit(s"$currentAttrPath${if (isArrayElement) "[*]" else ""}")))).otherwise(
          when((thisCol isNotNull) and primitiveCastErrorLogic(field, origSchema, currentAttrPath, thisCol), array(callUDF("stdCastErr", lit(s"$currentAttrPath${if (isArrayElement) "[*]" else ""}"), thisCol.cast(StringType))))
            .otherwise(typedLit(Seq[ErrorMessage]())))

        val std = when((size(err) === lit(0)) and (thisCol isNotNull), primitiveCastLogic(field, origSchema, currentAttrPath, thisCol)).
          otherwise(when(size(err) === lit(0), null).otherwise(default)) as field.name

        ParseOutput(std, err)
      }

      case t => throw new IllegalStateException(s"${t.typeName} is not a supported type in this version of Enceladus")
    }
  }

}