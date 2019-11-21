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
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults, TypedStructField}
import za.co.absa.enceladus.standardization.interpreter.stages.TypeParserSuiteTemplate._
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.time.DateTimePattern

trait TypeParserSuiteTemplate extends FunSuite with SparkTestBase {

  private implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
  private implicit val defaults: Defaults = GlobalDefaults

  protected def createCastTemplate(toType: DataType, pattern: String, timezone: Option[String]): String
  protected def createErrorCondition(srcField: String, target: StructField, castS: String):String

  private val sourceFieldName = "sourceField"

  protected val log: Logger = LogManager.getLogger(this.getClass)

  protected def doTestWithinColumnNullable(input: Input): Unit = {
    import input._
    val nullable = true
    val field = sourceField(baseType, nullable)
    val schema = buildSchema(Array(field), path)
    testTemplate(field, schema, path)
  }

  protected def doTestWithinColumnNotNullable(input: Input): Unit = {
    import input._
    val nullable = false
    val field = sourceField(baseType, nullable)
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
    val integerField = StructField("integerField", IntegerType, nullable = true,
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
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", DateTimePattern.EpochKeyword).build)
    val schema = buildSchema(Array(sourceField(baseType), dateField), path)
    testTemplate(dateField, schema, path, DateTimePattern.EpochKeyword)
  }

  protected def doTestIntoTimestampFieldWithEpochPattern(input: Input): Unit = {
    import input._
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", sourceFieldName).putString("pattern", DateTimePattern.EpochMilliKeyword).build)
    val schema = buildSchema(Array(sourceField(baseType), timestampField), path)
    testTemplate(timestampField, schema, path, DateTimePattern.EpochMilliKeyword)
  }

  private def sourceField(baseType: DataType, nullable: Boolean = true): StructField = StructField(sourceFieldName, baseType, nullable)

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
    val castString = createCastTemplate(target.dataType, pattern, timezone).format(srcField, srcField)
    val errColumnExpression = assembleErrorExpression(srcField, target, castString)
    val stdCastExpression = assembleCastExpression(srcField, target, castString, errColumnExpression)
    val output: ParseOutput = TypeParser.standardize(target, path, schema)

    doAssert(errColumnExpression, output.errors.toString())
    doAssert(stdCastExpression, output.stdCol.toString())
  }

  private def fullName(path: String, fieldName: String): String = {
    if (path.nonEmpty) s"$path.$fieldName" else fieldName
  }

  private def assembleCastExpression(srcField: String,
                                     target: StructField,
                                     castExpression: String,
                                     errorExpression: String): String = {
    val defaultValue = TypedStructField(target).defaultValueWithGlobal.get
    val default = defaultValue match {
      case Some(d: Date) => s"DATE '${d.toString}'"
      case Some(t: Timestamp) => s"TIMESTAMP('${t.toString}')"
      case Some(s: String) => s
      case Some(x) => x.toString
      case None => "NULL"
    }

    s"CASE WHEN (size($errorExpression) > 0) THEN $default ELSE CASE WHEN ($srcField IS NOT NULL) THEN $castExpression END END AS `${target.name}`"
  }

  private def assembleErrorExpression(srcField: String, target: StructField, castS: String): String = {
    val errCond = createErrorCondition(srcField, target, castS)

    if (target.nullable) {
      s"CASE WHEN (($srcField IS NOT NULL) AND ($errCond)) THEN array(stdCastErr($srcField, CAST($srcField AS STRING))) ELSE [] END"
    } else {
      s"CASE WHEN ($srcField IS NULL) THEN array(stdNullErr($srcField)) ELSE CASE WHEN ($errCond) THEN array(stdCastErr($srcField, CAST($srcField AS STRING))) ELSE [] END END"
    }
  }

  private def doAssert(expectedExpression: String, actualExpression: String): Unit = {
    if (actualExpression != expectedExpression) {
      // the expressions tend to be rather long, the assert most often cuts the beginning and/or end of the string
      // showing just the vicinity of the difference, so we log the output of the whole strings
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
