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

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.common.error.ErrorMessageFactory
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.schema.MetadataKeys
import za.co.absa.enceladus.utils.udf.UDFLibrary
import za.co.absa.enceladus.utils.validation.ValidationException

class StandardizationInterpreter_ArraySuite extends FunSuite with SparkTestBase with LoggerTestBase {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

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

    val expectedData =
      """+---------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||arrayField                                                     |errCol                                                                                                                                                                                                                                                                                                         |
        |+---------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||[,,]                                                           |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [00:00:00 01.12.2018], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [00:10:00 02.12.2018], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [00:20:00 03.12.2018], []]]|
        ||[,,]                                                           |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [00:00:00 01.12.2019], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [00:10:00 02.12.2019], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [00:20:00 03.12.2019], []]]|
        ||[2020-01-12 00:00:00, 2020-12-02 00:10:00, 2020-12-03 00:20:00]|[]                                                                                                                                                                                                                                                                                                             |
        |+---------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(
      "root\n"+
      " |-- arrayField: array (nullable = true)\n" +
      " |    |-- element: timestamp (containsNull = true)"
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    assert(std.schema.treeString == expectedSchema)
    assert(std.dataAsString(false) == expectedData)
  }

  test("Array of timestamps with pattern defined") {
    val seq  = Seq(
      Array("00:00:00 01.12.2008", "00:10:00 02.12.2008","00:20:00 03.12.2008"),
      Array("00:00:00 01.12.2009", "00:10:00 02.12.2009","00:20:00 03.12.2009"),
      Array("2010-01-12 00:00:00" ,"2010-12-02 00:10:00","2010-12-03 00:20:00")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(TimestampType, s""""${MetadataKeys.Pattern}": "HH:mm:ss dd.MM.yyyy"""")

    val expectedData =
      """+---------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||arrayField                                                     |errCol                                                                                                                                                                                                                                                                                                         |
        |+---------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||[2008-12-01 00:00:00, 2008-12-02 00:10:00, 2008-12-03 00:20:00]|[]                                                                                                                                                                                                                                                                                                             |
        ||[2009-12-01 00:00:00, 2009-12-02 00:10:00, 2009-12-03 00:20:00]|[]                                                                                                                                                                                                                                                                                                             |
        ||[,,]                                                           |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [2010-01-12 00:00:00], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [2010-12-02 00:10:00], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [2010-12-03 00:20:00], []]]|
        |+---------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(
    "root\n"+
      " |-- arrayField: array (nullable = true)\n" +
      " |    |-- element: timestamp (containsNull = true)"
    )
    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    assert(std.schema.treeString == expectedSchema)
    assert(std.dataAsString(false) == expectedData)
  }

  test("Array of timestamps with invalid pattern") {
    val seq  = Seq(
      Array("00:00:00 01.12.2013", "00:10:00 02.12.2013","00:20:00 03.12.2013"),
      Array("00:00:00 01.12.2014", "00:10:00 02.12.2014","00:20:00 03.12.2014"),
      Array("2015-01-12 00:00:00" ,"2015-12-02 00:10:00","2015-12-03 00:20:00")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(TimestampType, s""""${MetadataKeys.Pattern}": "fubar"""")
    assert(intercept[ValidationException] {
      StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    }.getMessage == "A fatal schema validation error occurred.")
  }

  test("Array of integers with pattern defined") {
    val seq  = Seq(
      Array("Size: 1", "Size: 2","Size: 3"),
      Array("Size: -7", "Size: ~13.13"),
      Array("A" , null, "")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(IntegerType, s""""${MetadataKeys.Pattern}": "Size: #;Size: -#"""")

    val expectedData =
      """+----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||arrayField|errCol                                                                                                                                                               |
        |+----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||[1, 2, 3] |[]                                                                                                                                                                   |
        ||[-7,]     |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [Size: ~13.13], []]]                                                                       |
        ||[,,]      |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [A], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [], []]]|
        |+----------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(
      "root\n"+
        " |-- arrayField: array (nullable = true)\n" +
        " |    |-- element: integer (containsNull = true)"
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    assert(std.schema.treeString == expectedSchema)
    assert(std.dataAsString(false) == expectedData)
  }

  test("Array of floats with minus sign changed and default defined") {
    val seq  = Seq(
      Array("1.1", "2.2","3.3"),
      Array("~7.7", "-13.13"),
      Array("A" , null, "")
    )
    val src = seq.toDF(fieldName)
    val desiredSchema = generateDesiredSchema(FloatType, s""""${MetadataKeys.DefaultValue}": "3.14", "${MetadataKeys.MinusSign}": "~" """)

    val expectedData =
      """+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||arrayField     |errCol                                                                                                                                                               |
        |+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        ||[1.1, 2.2, 3.3]|[]                                                                                                                                                                   |
        ||[-7.7, 3.14]   |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [-13.13], []]]                                                                             |
        ||[3.14,, 3.14]  |[[stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [A], []], [stdCastError, E00000, Standardization Error - Type cast, arrayField[*], [], []]]|
        |+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(
      "root\n"+
        " |-- arrayField: array (nullable = true)\n" +
        " |    |-- element: float (containsNull = true)"
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    assert(std.schema.treeString == expectedSchema)
    assert(std.dataAsString(false) == expectedData)
  }

  test("Array of arrays of string") {
    val seq = Seq(
      s"""{"$fieldName": [["a", "bb", "ccc"],["1", "12"],["Hello", null, "World"]]}"""
    )
    val src = JsonUtils.getDataFrameFromJson(spark, seq)

    val subArrayJson = """{"type": "array", "elementType": "string", "containsNull": false}"""
    val desiredSchema = generateDesiredSchema(subArrayJson, s""""${MetadataKeys.DefaultValue}": "Nope"""")

    val expectedData =
      """+---------------------------------------------+--------------------------------------------------------------------------------------------------------------------+
        ||arrayField                                   |errCol                                                                                                              |
        |+---------------------------------------------+--------------------------------------------------------------------------------------------------------------------+
        ||[[a, bb, ccc], [1, 12], [Hello, Nope, World]]|[[stdNullError, E00002, Standardization Error - Null detected in non-nullable attribute, arrayField[*], [null], []]]|
        |+---------------------------------------------+--------------------------------------------------------------------------------------------------------------------+
        |
        |""".stripMargin.replace("\r\n", "\n")
    val expectedSchema = ErrorMessageFactory.attachErrColToSchemaPrint(
      "root\n"+
        " |-- arrayField: array (nullable = true)\n" +
        " |    |-- element: array (containsNull = true)\n" +
        " |    |    |-- element: string (containsNull = true)"
    )

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()

    assert(std.schema.treeString == expectedSchema)
    assert(std.dataAsString(false) == expectedData)
  }
}
