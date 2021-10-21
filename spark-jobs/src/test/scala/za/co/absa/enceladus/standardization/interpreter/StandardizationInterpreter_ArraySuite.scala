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

package za.co.absa.enceladus.standardization.interpreter

import java.sql.Timestamp

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.common.error.ErrorMessageFactory
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils._
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}
import za.co.absa.enceladus.utils.udf.UDFLibrary
import za.co.absa.enceladus.utils.validation.ValidationException

class StandardizationInterpreter_ArraySuite extends AnyFunSuite with SparkTestBase with LoggerTestBase with Matchers with DatasetComparer {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary
  private implicit val defaults: Defaults = GlobalDefaults

  private val fieldName = "arrayField"

  private def generateDesiredSchema(arrayElementType: String, metadata: String): StructType = {
    val jsonField: String =  s"""{"name": "$fieldName", "type": { "type": "array", "elementType": $arrayElementType, "containsNull": true}, "nullable": true, "metadata": {$metadata} }"""
    val fullJson = s"""{"type": "struct", "fields": [$jsonField]}"""
    DataType.fromJson(fullJson).asInstanceOf[StructType]
  }

  private def generateDesiredSchema(arrayElementType: DataType, metadata: String = ""): StructType = {
    generateDesiredSchema('"' + arrayElementType.typeName + '"', metadata)
  }

  test("Array of timestamps with no pattern") {
    val seq  = Seq(
      Array("00:00:00 01.12.2018", "00:10:00 02.12.2018","00:20:00 03.12.2018"),
      Array("00:00:00 01.12.2019", "00:10:00 02.12.2019","00:20:00 03.12.2019"),
      Array("2020-01-12 00:00:00" ,"2020-12-02 00:10:00","2020-12-03 00:20:00")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(TimestampType)

    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(nullable = false,
      "root\n"+
      " |-- arrayField: array (nullable = true)\n" +
      " |    |-- element: timestamp (containsNull = true)"
    )

    val stdDF = StandardizationInterpreter.standardize(src, desiredSchema, "", errorColNullability = false).cache()
    assert(stdDF.schema.treeString == expectedSchema) // checking schema first

    val expectedData = Seq(
      Row(Seq(null, null, null), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("00:00:00 01.12.2018"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("00:10:00 02.12.2018"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("00:20:00 03.12.2018"), Seq())
      )),
      Row(Seq(null, null, null), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("00:00:00 01.12.2019"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("00:10:00 02.12.2019"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("00:20:00 03.12.2019"), Seq())
      )),
      Row(Seq(Timestamp.valueOf("2020-01-12 00:00:00"), Timestamp.valueOf("2020-12-02 00:10:00"), Timestamp.valueOf("2020-12-03 00:20:00")), Seq())
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema) // checking just the data
    assertSmallDatasetEquality(stdDF, expectedDF)
  }

  test("Array of timestamps with pattern defined") {
    val seq  = Seq(
      Array("00:00:00 01.12.2008", "00:10:00 02.12.2008","00:20:00 03.12.2008"),
      Array("00:00:00 01.12.2009", "00:10:00 02.12.2009","00:20:00 03.12.2009"),
      Array("2010-01-12 00:00:00" ,"2010-12-02 00:10:00","2010-12-03 00:20:00")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(TimestampType, s""""${MetadataKeys.Pattern}": "HH:mm:ss dd.MM.yyyy"""")

    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(nullable = true,
    "root\n"+
      " |-- arrayField: array (nullable = true)\n" +
      " |    |-- element: timestamp (containsNull = true)"
    )
    val stdDF = StandardizationInterpreter.standardize(src, desiredSchema, "", errorColNullability = true).cache()
    assert(stdDF.schema.treeString == expectedSchema) // checking schema first

    val expectedData = Seq(
      Row(Seq(Timestamp.valueOf("2008-12-01 00:00:00"), Timestamp.valueOf("2008-12-02 00:10:00"), Timestamp.valueOf("2008-12-03 00:20:00")), Seq()),
      Row(Seq(Timestamp.valueOf("2009-12-01 00:00:00"), Timestamp.valueOf("2009-12-02 00:10:00"), Timestamp.valueOf("2009-12-03 00:20:00")), Seq()),
      Row(Seq(null, null, null), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("2010-01-12 00:00:00"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("2010-12-02 00:10:00"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("2010-12-03 00:20:00"), Seq())
      ))
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema) // checking just the data
    assertSmallDatasetEquality(stdDF, expectedDF)
  }

  test("Array of timestamps with invalid pattern") {
    val seq  = Seq(
      Array("00:00:00 01.12.2013", "00:10:00 02.12.2013","00:20:00 03.12.2013"),
      Array("00:00:00 01.12.2014", "00:10:00 02.12.2014","00:20:00 03.12.2014"),
      Array("2015-01-12 00:00:00" ,"2015-12-02 00:10:00","2015-12-03 00:20:00")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(TimestampType, s""""${MetadataKeys.Pattern}": "fubar"""")
    val caught = intercept[ValidationException] {
      StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    }

    caught.getMessage should startWith ("A fatal schema validation error occurred.")
    caught.errors.head should startWith ("Validation error for column 'arrayField[].arrayField', pattern 'fubar")
  }

  test("Array of integers with pattern defined") {
    val seq  = Seq(
      Array("Size: 1", "Size: 2","Size: 3"),
      Array("Size: -7", "Size: ~13.13"),
      Array("A" , null, "")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(IntegerType, s""""${MetadataKeys.Pattern}": "Size: #;Size: -#"""")

    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(nullable = false,
      "root\n"+
        " |-- arrayField: array (nullable = true)\n" +
        " |    |-- element: integer (containsNull = true)"
    )

    val stdDF = StandardizationInterpreter.standardize(src, desiredSchema, "", errorColNullability = false).cache()
    assert(stdDF.schema.treeString == expectedSchema) // checking schema first

    val expectedData = Seq(
      Row(Seq(1,2,3), Seq()),
      Row(Seq(-7, null), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("Size: ~13.13"), Seq()),
      )),
      Row(Seq(null, null, null), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("A"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq(""), Seq())
      ))
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema) // checking just the data
    assertSmallDatasetEquality(stdDF, expectedDF)
  }

  test("Array of floats with minus sign changed and default defined") {
    val seq  = Seq(
      Array("1.1", "2.2","3.3"),
      Array("~7.7", "-13.13"),
      Array("A" , null, "")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(FloatType, s""""${MetadataKeys.DefaultValue}": "3.14", "${MetadataKeys.MinusSign}": "~" """)

    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(nullable = false,
      "root\n"+
        " |-- arrayField: array (nullable = true)\n" +
        " |    |-- element: float (containsNull = true)"
    )

    val stdDF = StandardizationInterpreter.standardize(src, desiredSchema, "", errorColNullability = false).cache()
    assert(stdDF.schema.treeString == expectedSchema) // checking schema first

    val expectedData = Seq(
      Row(Seq(1.1F, 2.2F, 3.3F), Seq()),
      Row(Seq(-7.7F, 3.14F), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("-13.13"), Seq()),
      )),
      Row(Seq(3.14F, null, 3.14F), Seq(
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq("A"), Seq()),
        Row("stdCastError", "E00000", "Standardization Error - Type cast", "arrayField[*]", Seq(""), Seq())
      ))
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema) // checking just the data
    assertSmallDatasetEquality(stdDF, expectedDF)
  }

  test("Array of arrays of string") {
    val seq = Seq(
      s"""{"$fieldName": [["a", "bb", "ccc"],["1", "12"],["Hello", null, "World"]]}"""
    )
    val src = JsonUtils.getDataFrameFromJson(spark, seq)

    val subArrayJson = """{"type": "array", "elementType": "string", "containsNull": false}"""
    val desiredSchema = generateDesiredSchema(subArrayJson, s""""${MetadataKeys.DefaultValue}": "Nope"""")

    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(nullable = true,
      "root\n"+
        " |-- arrayField: array (nullable = true)\n" +
        " |    |-- element: array (containsNull = true)\n" +
        " |    |    |-- element: string (containsNull = true)"
    )

    val stdDF = StandardizationInterpreter.standardize(src, desiredSchema, "", errorColNullability = true).cache()
    assert(stdDF.schema.treeString == expectedSchema) // checking schema first

    val expectedData = Seq(
      Row(Seq(Seq("a", "bb", "ccc"), Seq("1", "12"), Seq("Hello", "Nope", "World")), Seq(
        Row("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute", "arrayField[*]", Seq("null"), Seq()),
      ))
    )
    val expectedDF = expectedData.toDfWithSchema(stdDF.schema) // checking just the data
    assertSmallDatasetEquality(stdDF, expectedDF)
  }
}
