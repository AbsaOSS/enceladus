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
import java.sql.{Date, Timestamp}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.types.Defaults
import za.co.absa.enceladus.standardization.interpreter.stages.TypeParserSuiteTemplate._

trait TypeParserSuiteTemplate extends FunSuite {
  protected def createCastTemplate(toType: DataType, pattern: String, timezone: Option[String]): String

  private val sourceFieldName = "sourceField"
  private val dateEpochPattern = "epoch"
  private val timestampEpochPattern = "epochmilli"

  protected val log: Logger = LogManager.getLogger(this.getClass)

  protected def doTestWithinColumn(input: Input): Unit = {
    import input._
    val field = sourceField(baseType)
    val schema = buildSchema(Array(field), path)
    testTemplate(field, schema, path)
  }

  protected def doTestIntoStringField(input: Input): Unit = {
    import input._
    val stringField = StructField("stringField", StringType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn",sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), stringField), path)
    testTemplate(stringField, schema, path)
  }

  protected def doTestIntoFloatField(input: Input): Unit = {
    import input._
    val floatField = StructField("floatField", FloatType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), floatField), path)
    testTemplate(floatField, schema, path)
  }

  protected def doTestIntoIntegerField(input: Input): Unit = {
    import input._
    val integerField = StructField("integerField", IntegerType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), integerField), path)
    testTemplate(integerField, schema, path)
  }

  protected def doTestIntoBooleanField(input: Input): Unit = {
    import input._
    val booleanField = StructField("booleanField", BooleanType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), booleanField), path)
    testTemplate(booleanField, schema, path)
  }

  protected def doTestIntoDateFieldNoPattern(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)

    if (datetimeNeedsPattern) {
      val errMessage = s"Dates & times represented as ${baseType.typeName} values need specified 'pattern' metadata"
      val caughtErr = intercept[InvalidParameterException] {
        TypeParser.standardize(dateField, path, schema)
      }
      assert(caughtErr.getMessage == errMessage)
    } else {
      testTemplate(dateField, schema, path, "yyyy-MM-dd")
    }
  }

  protected def doTestIntoTimestampFieldNoPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)

    if (datetimeNeedsPattern) {
      val errMessage = s"Dates & times represented as ${baseType.typeName} values need specified 'pattern' metadata"
      val caughtErr = intercept[InvalidParameterException] {
        TypeParser.standardize(timestampField, path, schema)
      }
      assert(caughtErr.getMessage == errMessage)
    } else {
      testTemplate(timestampField, schema, path, "yyyy-MM-dd HH:mm:ss")
    }
  }

  protected def doTestIntoDateFieldWithPattern(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", datePattern).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, datePattern)
  }

  protected def doTestIntoTimestampFieldWithPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampPattern).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern)
  }

  protected def doTestIntoDateFieldWithPatternAndDefault(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", datePattern).putString("default", defaultValueDate).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, datePattern)
  }

  protected def doTestIntoTimestampFieldWithPatternAndDefault(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampPattern).putString("default", defaultValueTimestamp).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern)
  }

  protected def doTestIntoDateFieldWithPatternAndTimeZone(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", datePattern).putString("timezone", fixedTimezone).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, datePattern, Option(fixedTimezone))
  }

  protected def doTestIntoTimestampFieldWithPatternAndTimeZone(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampPattern).putString("timezone", fixedTimezone).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampPattern, Option(fixedTimezone))
  }

  protected def doTestIntoDateFieldWithEpochPattern(input: Input): Unit = {
    import input._
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", dateEpochPattern).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, dateEpochPattern)
  }

  protected def doTestIntoTimestampFieldWithEpochPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", timestampEpochPattern).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, timestampEpochPattern)
  }

  private def sourceField(baseType: DataType): StructField = StructField(sourceFieldName, baseType)

  private def buildSchema(fields: Array[StructField], path: String): StructType = {
    val innerSchema = StructType(fields)

    if (path.nonEmpty) {
      StructType(Array(StructField(path, innerSchema)))
    } else {
      innerSchema
    }
  }

  private def testTemplate(target: StructField, schema: StructType, path: String, pattern: String = "", timezone: Option[String] = None): Unit = {
    val srcField = fullName(path, sourceFieldName)
    val nullableS = (!target.nullable).toString
    val castString = createCastTemplate(target.dataType, pattern, timezone).format(srcField)
    val stdCastExpression = assemblyCastExpression(srcField, nullableS, castString, target)
    val errColumnExpression = assemblyErrorExpression(srcField, nullableS, castString)
    val output: ParseOutput = TypeParser.standardize(target, path, schema)

    doAssert(stdCastExpression, output.stdCol.toString())
    doAssert(errColumnExpression, output.errors.toString())
  }

  private def fullName(path: String, fieldName: String): String = {
    if (path.nonEmpty) s"$path.$fieldName" else fieldName
  }

  private def assemblyCastExpression(srcField: String, nullableS: String, castS: String, target: StructField): String = {
    val default = Defaults.getDefaultValue(target) match {
      case d: Date => s"DATE '${d.toString}'"
      case t: Timestamp => s"TIMESTAMP('${t.toString}')"
      case s: String => s
      case x => x.toString
    }
    s"CASE WHEN ((size(CASE WHEN (($srcField IS NULL) AND $nullableS) THEN array(stdNullErr($srcField)) ELSE CASE WHEN (($srcField IS NOT NULL) AND ($castS IS NULL)) THEN array(stdCastErr($srcField, CAST($srcField AS STRING))) ELSE [] END END) = 0) AND ($srcField IS NOT NULL)) THEN $castS ELSE CASE WHEN (size(CASE WHEN (($srcField IS NULL) AND $nullableS) THEN array(stdNullErr($srcField)) ELSE CASE WHEN (($srcField IS NOT NULL) AND ($castS IS NULL)) THEN array(stdCastErr($srcField, CAST($srcField AS STRING))) ELSE [] END END) = 0) THEN NULL ELSE $default END END AS `${target.name}`"
  }

  private def assemblyErrorExpression(srcField: String, nullableS: String, castS: String): String = {
    s"CASE WHEN (($srcField IS NULL) AND $nullableS) THEN array(stdNullErr($srcField)) ELSE CASE WHEN (($srcField IS NOT NULL) AND ($castS IS NULL)) THEN array(stdCastErr($srcField, CAST($srcField AS STRING))) ELSE [] END END"
  }

  private def doAssert(expectedExpression: String, actualExpression: String): Unit = {
    if (actualExpression != expectedExpression) {
      //the expressions tend to be rather long, the assert most often cuts the beginning and/or end of the string
      // showing just the vicinity of the difference, therefor the output of the whole strings
      log.error(s"Expected: $expectedExpression")
      log.error(s"Actual  : $actualExpression")
      assert(actualExpression == expectedExpression)
    }
  }

}

object TypeParserSuiteTemplate {
  case class Input(baseType: DataType,
                   defaultValueDate: String,
                   defaultValueTimestamp: String,
                   datePattern: String,
                   timestampPattern: String,
                   fixedTimezone: String,
                   path: String,
                   datetimeNeedsPattern: Boolean = true)
}
