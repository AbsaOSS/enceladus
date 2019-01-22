/*
 * Copyright 2018-${today.year} ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.enceladus.utils.types

import org.scalatest.FunSuite
import java.sql.Date
import java.sql.Timestamp
import java.util.TimeZone


case class TestInputRow(id: Int, stringField: String)

class EnceladusDateParserSuite extends FunSuite{
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  test("EnceladusDateParser class epoch") {
    val parser: EnceladusDateParser = EnceladusDateParser("epoch")

    val value: String = "1547553153";
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(119, 0, 15) //2019-01-19
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(119, 0, 15, 11,52 , 33, 0 ) //2019-01-19 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("EnceladusDateParser class milliepoch") {
    val parser: EnceladusDateParser = EnceladusDateParser("milliepoch")

    val value: String = "1547553153198";
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(119, 0, 15) //2019-01-19
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(119, 0, 15, 11,52 , 33, 198000000 ) //2019-01-19 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  test("EnceladusDateParser class actual patter") {
    val parser: EnceladusDateParser = EnceladusDateParser("yyyy_MM_dd:HH.mm.ss")

    val value: String = "2019_01_15:11.52.33";
    val resultDate: Date = parser.parseDate(value)
    val expectedDate: Date = new Date(119, 0, 15) //2019-01-19
    assert(resultDate.toString == expectedDate.toString)

    val resultTimestamp: Timestamp = parser.parseTimestamp(value)
    val expectedTimestamp: Timestamp = new Timestamp(119, 0, 15, 11,52 , 33, 0 ) //2019-01-19 11:52:33
    assert(resultTimestamp.toString == expectedTimestamp.toString)
  }

  /*---
  test("") {

    import spark.implicits._
    val path: String = "path"

    val idField: StructField = StructField("id", IntegerType, nullable = false)
    val srcField: StructField = StructField("stringField", DateType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_c").build)
    val input: Seq[TestInputRow] = Seq(
      TestInputRow(1, "2001-12-24"),
      TestInputRow(4, "1970-01-01"),
      TestInputRow(9, "2019-01-15")
    )
    val inputData: DataFrame = spark.createDataFrame(input)
    val fieldName: String = SchemaUtils.getFieldNameOverriddenByMetadata(srcField)
    val currentAttrPath: String = appendPath(path, fieldName)

    val origSchema: StructType = StructType(Array(srcField))
    val column: Column = col(currentAttrPath)
    inputData.show(false)
    val result: Column = EnceladusDateParser.enceladus_to_date(column, srcField, origSchema, path)
    println(result)
  }
  */
}
