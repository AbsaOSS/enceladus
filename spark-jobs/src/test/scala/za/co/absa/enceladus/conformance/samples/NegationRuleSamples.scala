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

package za.co.absa.enceladus.conformance.samples

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule.NegationConformanceRule

object NegationRuleSamples {

  val schema = StructType(
    Array(
      StructField("byte", ByteType, nullable = false),
      StructField("short", ShortType, nullable = false),
      StructField("integer", IntegerType, nullable = false),
      StructField("long", LongType, nullable = false),
      StructField("float", FloatType, nullable = false),
      StructField("double", DoubleType, nullable = false),
      StructField("decimal", DecimalType(32, 2), nullable = false),
      StructField("date", DateType, nullable = false),
      StructField("boolean", BooleanType, nullable = false)
    )
  )

  val schemaNullable = StructType(
    schema.fields.map(_.copy(nullable = true))
  )

  val negateByte = NegationConformanceRule(order = 1, outputColumn = "ConformedByte", controlCheckpoint = false, inputColumn = "byte")
  val negateShort = NegationConformanceRule(order = 2, outputColumn = "ConformedShort", controlCheckpoint = false, inputColumn = "short")
  val negateInt = NegationConformanceRule(order = 3, outputColumn = "ConformedInt", controlCheckpoint = false, inputColumn = "integer")
  val negateLong = NegationConformanceRule(order = 4, outputColumn = "ConformedLong", controlCheckpoint = false, inputColumn = "long")
  val negateFloat = NegationConformanceRule(order = 5, outputColumn = "ConformedFloat", controlCheckpoint = false, inputColumn = "float")
  val negateDouble = NegationConformanceRule(order = 6, outputColumn = "ConformedDouble", controlCheckpoint = false, inputColumn = "double")
  val negateDecimal = NegationConformanceRule(order = 7, outputColumn = "ConformedDecimal", controlCheckpoint = false, inputColumn = "decimal")
  val negateBoolean = NegationConformanceRule(order = 8, outputColumn = "ConformedBoolean", controlCheckpoint = false, inputColumn = "boolean")

  val dataset = Dataset(name = "Test Name", version = 1, hdfsPath = "", hdfsPublishPath = "",
    schemaName = "Test Name", schemaVersion = 1,
    conformance = List(negateByte, negateShort, negateInt, negateLong, negateFloat, negateDouble, negateDecimal, negateBoolean))

  object Positive {
    val data: Seq[String] = Seq(
      s"""{ "byte": 1,
         |  "short": 1,
         |  "integer": 1,
         |  "long": 1,
         |  "float": 1.0,
         |  "double": 1.0,
         |  "decimal": 1.0,
         |  "date": "2018-06-11",
         |  "boolean": true}""".stripMargin)

    val conformedJSON = """{"byte":1,"short":1,"integer":1,"long":1,"float":1.0,"double":1.0,"decimal":1.00,"date":"2018-06-11","boolean":true,"errCol":[],"ConformedByte":-1,"ConformedShort":-1,"ConformedInt":-1,"ConformedLong":-1,"ConformedFloat":-1.0,"ConformedDouble":-1.0,"ConformedDecimal":-1.00,"ConformedBoolean":false}"""
  }

  object Negative {
    val data: Seq[String] = Seq(
      s"""{ "byte": -1,
         |  "short": -1,
         |  "integer": -1,
         |  "long": -1,
         |  "float": -1.0,
         |  "double": -1.0,
         |  "decimal": -1.0,
         |  "date": "2018-06-11",
         |  "boolean": false}""".stripMargin)

    val conformedJSON = """{"byte":-1,"short":-1,"integer":-1,"long":-1,"float":-1.0,"double":-1.0,"decimal":-1.00,"date":"2018-06-11","boolean":false,"errCol":[],"ConformedByte":1,"ConformedShort":1,"ConformedInt":1,"ConformedLong":1,"ConformedFloat":1.0,"ConformedDouble":1.0,"ConformedDecimal":1.00,"ConformedBoolean":true}"""
  }

  object Zero {
    val data: Seq[String] = Seq(
      s"""{ "byte": 0,
         |  "short": 0,
         |  "integer": 0,
         |  "long": 0,
         |  "float": 0.0,
         |  "double": 0.0,
         |  "decimal": 0.0,
         |  "date": "2018-06-11",
         |  "boolean": true}""".stripMargin)

    val conformedJSON = """{"byte":0,"short":0,"integer":0,"long":0,"float":0.0,"double":0.0,"decimal":0.00,"date":"2018-06-11","boolean":true,"errCol":[],"ConformedByte":0,"ConformedShort":0,"ConformedInt":0,"ConformedLong":0,"ConformedFloat":0.0,"ConformedDouble":0.0,"ConformedDecimal":0.00,"ConformedBoolean":false}"""
  }

  object Min {
    val data: Seq[String] = Seq(
      s"""{ "byte": ${Byte.MinValue},
         |  "short": ${Short.MinValue},
         |  "integer": ${Int.MinValue},
         |  "long": ${Long.MinValue},
         |  "float": ${Float.MinValue},
         |  "double": ${Double.MinValue},
         |  "decimal": 0,
         |  "date": "2018-06-11",
         |  "boolean": true}""".stripMargin)

    val conformedJSON = """{"byte":-128,"short":-32768,"integer":-2147483648,"long":-9223372036854775808,"float":-3.4028235E38,"double":-1.7976931348623157E308,"decimal":0.00,"date":"2018-06-11","boolean":true,"errCol":[{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedByte","rawValues":["-128"],"mappings":[]},{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedShort","rawValues":["-32768"],"mappings":[]},{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedInt","rawValues":["-2147483648"],"mappings":[]},{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedLong","rawValues":["-9223372036854775808"],"mappings":[]}],"ConformedByte":0,"ConformedShort":0,"ConformedInt":0,"ConformedLong":0,"ConformedFloat":3.4028234663852886E38,"ConformedDouble":1.7976931348623157E308,"ConformedDecimal":0.00,"ConformedBoolean":false}"""
  }

  object MinWithNullableColumns {
    val data: Seq[String] = Seq(
      s"""{ "byte": ${Byte.MinValue},
         |  "short": ${Short.MinValue},
         |  "integer": ${Int.MinValue},
         |  "long": ${Long.MinValue},
         |  "float": ${Float.MinValue},
         |  "double": ${Double.MinValue},
         |  "decimal": 0,
         |  "date": "2018-06-11",
         |  "boolean": true}""".stripMargin)

    val conformedJSON = """{"byte":-128,"short":-32768,"integer":-2147483648,"long":-9223372036854775808,"float":-3.4028235E38,"double":-1.7976931348623157E308,"decimal":0.00,"date":"2018-06-11","boolean":true,"errCol":[{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedByte","rawValues":["-128"],"mappings":[]},{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedShort","rawValues":["-32768"],"mappings":[]},{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedInt","rawValues":["-2147483648"],"mappings":[]},{"errType":"confNegError","errCode":"E00004","errMsg":"Conformance Error - Negation of numeric type with minimum value overflows and remains unchanged","errCol":"ConformedLong","rawValues":["-9223372036854775808"],"mappings":[]}],"ConformedFloat":3.4028234663852886E38,"ConformedDouble":1.7976931348623157E308,"ConformedDecimal":0.00,"ConformedBoolean":false}"""
  }

  object Max {
    val data: Seq[String] = Seq(
      s"""{ "byte": ${Byte.MaxValue},
         |  "short": ${Short.MaxValue},
         |  "integer": ${Int.MaxValue},
         |  "long": ${Long.MaxValue},
         |  "float": ${Float.MaxValue},
         |  "double": ${Double.MaxValue},
         |  "decimal": 0,
         |  "date": "2018-06-11",
         |  "boolean": true}""".stripMargin)

    val conformedJSON = """{"byte":127,"short":32767,"integer":2147483647,"long":9223372036854775807,"float":3.4028235E38,"double":1.7976931348623157E308,"decimal":0.00,"date":"2018-06-11","boolean":true,"errCol":[],"ConformedByte":-127,"ConformedShort":-32767,"ConformedInt":-2147483647,"ConformedLong":-9223372036854775807,"ConformedFloat":-3.4028234663852886E38,"ConformedDouble":-1.7976931348623157E308,"ConformedDecimal":0.00,"ConformedBoolean":false}"""
  }

  object Null {
    val data: Seq[String] = Seq(
      s"""{ "byte": null,
         |  "short": null,
         |  "integer": null,
         |  "long": null,
         |  "float": null,
         |  "double": null,
         |  "decimal": null,
         |  "date": null,
         |  "boolean": null}""".stripMargin
    )

    // `.toJSON()` discards null-valued fields
    val conformedJSON = """{"errCol":[]}"""
  }

}
