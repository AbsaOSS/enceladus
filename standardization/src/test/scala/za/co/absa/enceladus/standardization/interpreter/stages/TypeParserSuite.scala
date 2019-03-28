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
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.interpreter.dataTypes.ParseOutput
import za.co.absa.enceladus.utils.error.UDFLibrary
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.time.{DateTimePattern, EnceladusDateTimeParser}
import za.co.absa.enceladus.utils.types.Defaults
import org.apache.spark.sql.functions._
import scala.util.Random

class TypeParserSuite extends FunSuite with SparkTestBase {

  implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary

  val showStandardization: Boolean = false

  private case class DS(precision: Int, scale: Int) //DecimalSize

  private type TestData = Map[DataType, Seq[String]]

  private case class TestInput(
    defaultValueDate: String,
    defaultValueTimestamp: String,
    datePattern: String,
    timestampPattern: String,
    timezone: String,
    baseType: DataType,
    path: String,
    datetimeNeedsPattern: Boolean = true,
    datePatternDS: DS = DS(0, 0),
    timestampPatternDS: DS = DS(0, 0),
    testData: TestData = Map.empty
  )

  private def fullName(path: String, fieldName: String): String = {
    if (path.nonEmpty) s"$path.$fieldName" else fieldName
  }

  private def testCast(output: ParseOutput, path: String, src: String, pattern: String, fromType: DataType, toType: DataType): Unit = {
    def expandPath(pathSegments: Seq[String], col: Column): Column = {
      pathSegments match {
        case Nil => col
        case _ => struct(expandPath(pathSegments.tail, col)).as(pathSegments.head)
      }
    }

    import spark.implicits._

    val srcF = fullName(path, src)
    val pathSeq: Seq[String] = if (path.isEmpty) {Nil} else  {path.split("\\.")}

    val dataString: Option[String] = (fromType, toType) match {
      case (BooleanType, _) => Some(Random.nextBoolean.toString)
      case (_, TimestampType) => Some(EnceladusDateTimeParser(pattern).format(new Timestamp(System.currentTimeMillis())))
      case (_, DateType) => Some(EnceladusDateTimeParser(pattern).format(new Date(System.currentTimeMillis())))
      case (_, IntegerType) => Some(Random.nextInt.toString)
      case (_, LongType) => Some(Random.nextLong.toString)
      case (IntegerType, FloatType) |
           (IntegerType, DoubleType) |
           (LongType, FloatType) |
           (LongType, DoubleType) => Some(Random.nextInt.toString)
      case (FloatType, FloatType) |
           (FloatType, DoubleType) |
           (DoubleType, FloatType) |
           (DoubleType, DoubleType) |
           (FloatType, DecimalType()) |
           (DoubleType, DecimalType()) |
           (DecimalType(), FloatType) |
           (DecimalType(), DoubleType)  => Some(Random.nextFloat.toString)
      case (StringType, StringType) => Some(Random.nextString(25))
      case _ =>
        if (showStandardization) {
          println(s"No data generation method for '${fromType.typeName}'->'${toType.typeName}' standardization")
        }
        None
    }

    dataString.foreach( data => {
      val srcDF = Seq(data)
        .toDF("raw")
        .select(expandPath(pathSeq, col("raw").cast(fromType).as(src)))
      val convertedDF = srcDF.select(
        col(srcF),
        output.stdCol,
        output.errors
      )
      if (showStandardization) {
        convertedDF.printSchema()
        convertedDF.show(false)
      }
    })
  }

  private def createCastTemplate(fromType: DataType,
                                 toType: DataType,
                                 pattern: String,
                                 ds: DS,
                                 timezone: Option[String]
                                 ): String = {
    val isEpoch = DateTimePattern.isEpoch(pattern)
    (fromType, toType, isEpoch, timezone) match {
      case (_, DateType, true, _)                      => s"to_date(from_unixtime((CAST(`%s` AS BIGINT) / ${DateTimePattern.epochFactor(pattern)}L), 'yyyy-MM-dd'), 'yyyy-MM-dd')"
      case (_, TimestampType, true, _)                 => s"to_timestamp(from_unixtime((CAST(`%s` AS BIGINT) / ${DateTimePattern.epochFactor(pattern)}L), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss')"
      case (StringType, DateType, _, Some(tz))         => s"to_utc_timestamp(to_timestamp(`%s`, '$pattern'), $tz)"
      case (StringType, TimestampType, _, Some(tz))    => s"to_utc_timestamp(to_timestamp(`%s`, '$pattern'), $tz)"
      case (DoubleType, TimestampType, _, Some(tz))    => s"to_utc_timestamp(to_timestamp(CAST(CAST(`%s` AS DECIMAL(${ds.precision},${ds.scale})) AS STRING), '$pattern'), $tz)"
      case (FloatType, TimestampType, _, Some(tz))     => s"to_utc_timestamp(to_timestamp(CAST(CAST(`%s` AS DECIMAL(${ds.precision},${ds.scale})) AS STRING), '$pattern'), $tz)"
      case (DoubleType, DateType, _, Some(tz))         => s"to_utc_timestamp(to_timestamp(CAST(CAST(`%s` AS DECIMAL(${ds.precision},${ds.scale})) AS STRING), '$pattern'), $tz)"
      case (FloatType, DateType, _, Some(tz))          => s"to_utc_timestamp(to_timestamp(CAST(CAST(`%s` AS DECIMAL(${ds.precision},${ds.scale})) AS STRING), '$pattern'), $tz)"
      case (TimestampType, TimestampType, _, Some(tz)) => s"to_utc_timestamp(%s, $tz)"
      case (TimestampType, DateType, _, Some(tz))      => s"to_date(to_utc_timestamp(`%s`, '$tz'))"
      case (DateType, TimestampType, _, Some(tz))      => s"to_utc_timestamp(%s, $tz)"
      case (DateType, DateType, _, Some(tz))           => s"to_date(to_utc_timestamp(`%s`, '$tz'))"
      case (_, DateType, _, Some(tz))                  => s"to_utc_timestamp(to_timestamp(CAST(`%s` AS STRING), '$pattern'), $tz)"
      case (_, TimestampType, _, Some(tz))             => s"to_utc_timestamp(to_timestamp(CAST(`%s` AS STRING), '$pattern'), $tz)"
      case (TimestampType, TimestampType, _, _)        |
           (DateType, DateType, _, _)                  => "%s"
      case (DoubleType, TimestampType, _, _)           |
           (FloatType, TimestampType, _, _)            => s"to_timestamp(CAST(CAST(`%s` AS DECIMAL(${ds.precision},${ds.scale})) AS STRING), '$pattern')"
      case (DoubleType, DateType, _, _)                |
           (FloatType, DateType, _, _)                 => s"to_date(CAST(CAST(`%s` AS DECIMAL(${ds.precision},${ds.scale})) AS STRING), '$pattern')"
      case (StringType, TimestampType, _, _)           =>  s"to_timestamp(`%s`, '$pattern')"
      case (StringType, DateType, _, _)                => s"to_date(`%s`, '$pattern')"
      case (TimestampType, DateType, _, _)             => "to_date(`%s`)"
      case (DateType, TimestampType, _, _)             => "to_timestamp(`%s`)"
      case (_, TimestampType, _, _)                    => s"to_timestamp(CAST(`%s` AS STRING), '$pattern')"
      case (_, DateType, _, _)                         => s"to_date(CAST(`%s` AS STRING), '$pattern')"
      case _                                           => s"CAST(%s AS ${toType.sql})"
    }
  }

  private def checkOutput(output: ParseOutput,
                          path: String,
                          target: StructField,
                          baseType: DataType,
                          pattern: String = "",
                          decimalSize: DS = DS(0, 0),
                          timezone: Option[String] = None,
                          src: String = "sourceField",
                          testData: TestData = Map.empty
                         ): Unit = {

    val srcF = fullName(path, src)
    val boolS = (!target.nullable).toString

    val default = Defaults.getDefaultValue(target) match {
      case d: Date => s"DATE '${d.toString}'"
      case t: Timestamp => s"TIMESTAMP('${t.toString}')"
      case s: String => s
      case x => x.toString
    }

    val castString = createCastTemplate(baseType, target.dataType, pattern, decimalSize, timezone).format(srcF)

    val std = s"CASE WHEN ((size(CASE WHEN (($srcF IS NULL) AND $boolS) THEN array(stdNullErr($srcF)) ELSE CASE WHEN (($srcF IS NOT NULL) AND ($castString IS NULL)) THEN array(stdCastErr($srcF, CAST($srcF AS STRING))) ELSE [] END END) = 0) AND ($srcF IS NOT NULL)) THEN $castString ELSE CASE WHEN (size(CASE WHEN (($srcF IS NULL) AND $boolS) THEN array(stdNullErr($srcF)) ELSE CASE WHEN (($srcF IS NOT NULL) AND ($castString IS NULL)) THEN array(stdCastErr($srcF, CAST($srcF AS STRING))) ELSE [] END END) = 0) THEN NULL ELSE $default END END AS `${target.name}`"
    val actual = output.stdCol.toString().replaceFirst("#\\d+$", "")
    assert(actual == std)

    val err = s"CASE WHEN (($srcF IS NULL) AND $boolS) THEN array(stdNullErr($srcF)) ELSE CASE WHEN (($srcF IS NOT NULL) AND ($castString IS NULL)) THEN array(stdCastErr($srcF, CAST($srcF AS STRING))) ELSE [] END END"
    assert(output.errors.toString() == err)

    testCast(output, path, src, pattern, baseType, target.dataType)
  }

  private def testTemplate(input:TestInput): Unit = {
    val dateEpochPattern = "epoch"
    val timestampEpochPattern = "epochmilli"

    val sourceField = StructField("sourceField", input.baseType)
    val noMetaField = StructField("no_metaField", input.baseType, nullable = false,
      new MetadataBuilder().putString("meta", "data").build)
    val stringField = StructField("stringField", StringType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").build)
    val floatField = StructField("floatField", FloatType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").build)
    val integerField = StructField("integerField", IntegerType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").build)
    val booleanField = StructField("booleanField", BooleanType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").build)
    val dateField = StructField("dateField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").build)
    val timestampField = StructField("timestampField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").build)
    val datePatternField = StructField("datePatternField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", input.datePattern).build)
    val timestampPatternField = StructField("timestampPatternField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", input.timestampPattern).build)
    val datePatternDefaultField = StructField("datePatternDefaultField", DateType, nullable = true,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", input.datePattern).putString("default", input.defaultValueDate).build)
    val timestampPatternDefaultField = StructField("timestampPatternDefaultField", TimestampType, nullable = true,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", input.timestampPattern).putString("default", input.defaultValueTimestamp).build)
    val datePatternTmzField = StructField("datePatternTmzField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", input.datePattern).putString("timezone", input.timezone).build)
    val timestampPatternTmzField = StructField("timestampPatternTmzField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", input.timestampPattern).putString("timezone", input.timezone).build)
    val dateEpochField = StructField("dateEpochField", DateType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", dateEpochPattern).build)
    val timestampEpochField = StructField("timestampEpochField", TimestampType, nullable = false,
      new MetadataBuilder().putString("sourcecolumn", "sourceField").putString("pattern", timestampEpochPattern).build)

    val schema = buildSchema(input.path, Array(
      sourceField,
      noMetaField,
      stringField,
      floatField,
      integerField,
      booleanField,
      dateField,
      timestampField,
      datePatternField,
      timestampPatternField,
      datePatternDefaultField,
      timestampPatternDefaultField,
      datePatternTmzField,
      timestampPatternTmzField,
      dateEpochField,
      timestampEpochField
    ))

    var output: ParseOutput = TypeParser.standardize(sourceField, input.path, schema)
    checkOutput(output, input.path, sourceField, input.baseType)
    output = TypeParser.standardize(noMetaField, input.path, schema)
    checkOutput(output, input.path, noMetaField, input.baseType, src = "no_metaField")
    output = TypeParser.standardize(stringField, input.path, schema)
    checkOutput(output, input.path, stringField, input.baseType)
    output = TypeParser.standardize(floatField, input.path, schema)
    checkOutput(output, input.path, floatField, input.baseType)
    output = TypeParser.standardize(integerField, input.path, schema)
    checkOutput(output, input.path, integerField,input.baseType)
    output = TypeParser.standardize(booleanField, input.path, schema)
    checkOutput(output, input.path, booleanField, input.baseType)

    if (input.datetimeNeedsPattern) {
      var errMessage = s"Dates & times represented as ${input.baseType.typeName} values need specified 'pattern' metadata"
      var caughtErr = intercept[InvalidParameterException] {
        TypeParser.standardize(dateField, input.path, schema)
      }
      assert(caughtErr.getMessage == errMessage)
      errMessage = s"Dates & times represented as ${input.baseType.typeName} values need specified 'pattern' metadata"
      caughtErr = intercept[InvalidParameterException] {
        TypeParser.standardize(timestampField, input.path, schema)
      }
      assert(caughtErr.getMessage == errMessage)
    } else {
      output = TypeParser.standardize(dateField, input.path, schema)
      checkOutput(output, input.path, dateField,  input.baseType, "yyyy-MM-dd")
      output = TypeParser.standardize(timestampField, input.path, schema)
      checkOutput(output, input.path, timestampField, input.baseType, "yyyy-MM-dd HH:mm:ss")
    }
    output = TypeParser.standardize(datePatternField, input.path, schema)
    checkOutput(output, input.path, datePatternField, input.baseType, input.datePattern, input.datePatternDS)
    output = TypeParser.standardize(timestampPatternField, input.path, schema)
    checkOutput(output, input.path, timestampPatternField, input.baseType, input.timestampPattern, input.timestampPatternDS)
    output = TypeParser.standardize(datePatternDefaultField, input.path, schema)
    checkOutput(output, input.path, datePatternDefaultField, input.baseType, input.datePattern, input.datePatternDS)
    output = TypeParser.standardize(timestampPatternDefaultField, input.path, schema)
    checkOutput(output, input.path, timestampPatternDefaultField, input.baseType, input.timestampPattern, input.timestampPatternDS)
    output = TypeParser.standardize(datePatternTmzField, input.path, schema)
    checkOutput(output, input.path, datePatternTmzField, input.baseType, input.datePattern, input.datePatternDS, Some(input.timezone))
    output = TypeParser.standardize(timestampPatternTmzField, input.path, schema)
    checkOutput(output, input.path, timestampPatternTmzField, input.baseType, input.timestampPattern, input.timestampPatternDS, Some(input.timezone))
    output = TypeParser.standardize(dateEpochField, input.path, schema)
    checkOutput(output, input.path, dateEpochField, input.baseType, dateEpochPattern)
    output = TypeParser.standardize(timestampEpochField, input.path, schema)
    checkOutput(output, input.path, timestampEpochField, input.baseType, timestampEpochPattern)
  }

  private def buildSchema(path:String, fields: Array[StructField]): StructType = {
    val innerSchema = StructType(fields)

    if (path.nonEmpty) {
      StructType(Array(StructField(path, innerSchema)))
    } else {
      innerSchema
    }
  }

  test("From string source") {
    val input = TestInput(
      defaultValueDate = "01.01.1970",
      defaultValueTimestamp = "01.01.1970 00:00:00",
      datePattern = "dd.MM.yyyy",
      timestampPattern = "dd.MM.yyyy HH:mm:ss",
      timezone = "CET",
      baseType = StringType,
      path = "",
      datetimeNeedsPattern = false
    )
    testTemplate(input)
  }

  test("From long source") {
    val input = TestInput(
      defaultValueDate = "20001010",
      defaultValueTimestamp = "199912311201",
      datePattern = "yyyyMMdd",
      timestampPattern = "yyyyMMddHHmm",
      timezone = "EST",
      baseType = LongType,
      path = "Hey"
    )
    testTemplate(input)
  }

  test("From boolean source") {
    val input = TestInput(
      defaultValueDate = "0",
      defaultValueTimestamp = "1",
      datePattern = "u",
      timestampPattern = "F",
      timezone = "WST",
      baseType = BooleanType,
      path = "Boo"
    )
    testTemplate(input)
  }

  test("From decimal source") {
    val input = TestInput(
      defaultValueDate = "700101",
      defaultValueTimestamp = "991231.2359",
      datePattern = "yyMMdd",
      timestampPattern = "yyMMdd.HHmm",
      timezone = "CST",
      baseType = DecimalType(10, 4),
      path = "hello"
    )
    testTemplate(input)
  }


  test("From double source") {
    val input = TestInput(
      defaultValueDate = "7001.01",
      defaultValueTimestamp = "991231.2359",
      datePattern = "yyMM.dd",
      timestampPattern = "yyMMdd.HHmm",
      timezone = "CEST",
      baseType = DoubleType,
      path = "Double",
      datePatternDS = DS(6, 2),
      timestampPatternDS = DS(10, 4)
    )
    testTemplate(input)
  }


  test("From timestamp source") {
    val input = TestInput(
      defaultValueDate = "700101",
      defaultValueTimestamp = "991231.2359",
      datePattern = "yyMMdd",
      timestampPattern = "yyMMdd.HHmm",
      timezone = "CST",
      baseType = TimestampType,
      path = "timestamp",
      datetimeNeedsPattern = false
    )
    testTemplate(input)
  }

  test("From date source") {
    val input = TestInput(
      defaultValueDate = "700101",
      defaultValueTimestamp = "991231.2359",
      datePattern = "yyMMdd",
      timestampPattern = "yyMMdd.HHmm",
      timezone = "CST",
      baseType = DateType,
      path = "Date",
      datetimeNeedsPattern = false
    )
    testTemplate(input)
  }

  test("Test standardize with sourcecolumn metadata") {
    val structFieldNoMetadata = StructField("a", IntegerType)
    val structFieldWithMetadataNotSourceColumn = StructField("b", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
    val structFieldWithMetadataSourceColumn = StructField("c", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_c").build)
    val schema = StructType(Array(structFieldNoMetadata, structFieldWithMetadataNotSourceColumn, structFieldWithMetadataSourceColumn))
    //Just Testing field name override
    import spark.implicits._
    implicit val udfLib: UDFLibrary = new za.co.absa.enceladus.utils.error.UDFLibrary
    val parseOutputStructFieldNoMetadata = TypeParser.standardize(structFieldNoMetadata, "path", schema)
    assertResult(true)(parseOutputStructFieldNoMetadata.stdCol.expr.toString().contains("path.a"))
    assertResult(false)(parseOutputStructFieldNoMetadata.stdCol.expr.toString().replaceAll("path.a", "").contains("path"))
    assertResult(true)(parseOutputStructFieldNoMetadata.errors.expr.toString().contains("path.a"))
    assertResult(false)(parseOutputStructFieldNoMetadata.errors.expr.toString().replaceAll("path.a", "").contains("path"))
    val parseOutputStructFieldWithMetadataNotSourceColumn = TypeParser.standardize(structFieldWithMetadataNotSourceColumn, "path", schema)
    assertResult(true)(parseOutputStructFieldWithMetadataNotSourceColumn.stdCol.expr.toString().contains("path.b"))
    assertResult(false)(parseOutputStructFieldWithMetadataNotSourceColumn.stdCol.expr.toString().replaceAll("path.b", "").contains("path"))
    assertResult(true)(parseOutputStructFieldWithMetadataNotSourceColumn.errors.expr.toString().contains("path.b"))
    assertResult(false)(parseOutputStructFieldWithMetadataNotSourceColumn.errors.expr.toString().replaceAll("path.b", "").contains("path"))
    val parseOutputStructFieldWithMetadataSourceColumn = TypeParser.standardize(structFieldWithMetadataSourceColumn, "path",schema)
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.stdCol.expr.toString().contains("path.c"))
    assertResult(true)(parseOutputStructFieldWithMetadataSourceColumn.stdCol.expr.toString().contains("path.override_c"))
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.stdCol.expr.toString().replaceAll("path.override_c", "").contains("path"))
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.errors.expr.toString().contains("path.c"))
    assertResult(true)(parseOutputStructFieldWithMetadataSourceColumn.errors.expr.toString().contains("path.override_c"))
    assertResult(false)(parseOutputStructFieldWithMetadataSourceColumn.errors.expr.toString().replaceAll("path.override_c", "").contains("path"))
  }

}
