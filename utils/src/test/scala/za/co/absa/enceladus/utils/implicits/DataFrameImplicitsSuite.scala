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

package za.co.absa.enceladus.utils.implicits

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.utils.implicits.DataFrameImplicits.DataFrameEnhancements
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class DataFrameImplicitsSuite extends AnyFunSuite with SparkTestBase with Matchers {

  import spark.implicits._

  private val columnName = "data"
  private val inputDataSeq = Seq(
    "0123456789012345678901234",
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "i",
    "j",
    "k",
    "l",
    "m",
    "n",
    "o",
    "p",
    "q",
    "r",
    "s",
    "t",
    "u",
    "v",
    "w",
    "x",
    "y",
    "z"
  )
  private val inputData = inputDataSeq.toDF(columnName)

  private def cellText(text: String, width: Int, leftAlign: Boolean): String = {
    val pad = " " * (width - text.length)
    if (leftAlign) {
      text + pad
    } else {
      pad + text
    }
  }

  private def line(width: Int): String = {
    "+" + "-" * width + "+"
  }

  private def header(width: Int, leftAlign: Boolean): String = {
    val lineStr = line(width)
    val title = cellText(columnName, width, leftAlign)
    s"$lineStr\n|$title|\n$lineStr"
  }

  private def cell(text: String, width: Int, leftAlign: Boolean): String = {
    val inner = if (text.length > width) {
      text.substring(0, width - 3) + "..."
    } else {
      cellText(text, width, leftAlign)
    }
    s"|$inner|"
  }

  private def inputDataToString(width: Int, leftAlign: Boolean, limit: Option[Int] = Option(20)): String = {
    val (extraLine, seq) = limit match {
      case Some(n) =>
        val line = if (inputDataSeq.length > n) {
          s"only showing top $n rows\n"
        } else {
          ""
        }
        (line, inputDataSeq.take(n))
      case None    =>
        ("", inputDataSeq)
    }
    seq.foldLeft(header(width, leftAlign)) { (acc, item) =>
      acc + "\n" + cell(item, width, leftAlign)
    } + "\n" + line(width) + s"\n$extraLine\n"
  }

  test("Like show()") {
    val result = inputData.dataAsString()
    val leftAlign = false
    val cellWidth = 20
    val expected = inputDataToString(cellWidth, leftAlign)

    assert(result == expected)
  }

  test("Like show(false)") {
    val result = inputData.dataAsString(false)
    val leftAlign = true
    val cellWidth = 25
    val expected = inputDataToString(cellWidth, leftAlign)

    assert(result == expected)
  }

  test("Like show(3, true)") {
    val result = inputData.dataAsString(3, true)
    val leftAlign = false
    val cellWidth = 20
    val expected = inputDataToString(cellWidth, leftAlign, Option(3))

    assert(result == expected)
  }

  test("Like show(30, false)") {
    val result = inputData.dataAsString(30, false)
    val leftAlign = true
    val cellWidth = 25
    val expected = inputDataToString(cellWidth, leftAlign, Option(30))

    assert(result == expected)
  }


  test("Like show(10, 10)") {
    val result = inputData.dataAsString(10, 10)
    val leftAlign = false
    val cellWidth = 10
    val expected = inputDataToString(cellWidth, leftAlign, Option(10))

    assert(result == expected)
  }

  test("Like show(50, 50, false)") {
    val result = inputData.dataAsString(50, 50, false)
    val leftAlign = false
    val cellWidth = 25
    val expected = inputDataToString(cellWidth, leftAlign, Option(50))

    assert(result == expected)
  }

  { // nullability changing tests
    val testData = Seq(
      Row("James", "Java"),
      Row("Michael", "Python")
    )

    val originalSchema = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("language", StringType, nullable = false)
    ))

    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(testData), originalSchema)
    assert(df1.schema == originalSchema, "Nullability changing tests - initialization failed")

    test("changing nullability of a column") {
      val newSchema1 = df1.setNullableStateOfColumn("name", false).schema
      newSchema1.toSet should contain(StructField("name", StringType, nullable = false))

      val newSchema2 = df1.setNullableStateOfColumn("language", true).schema
      newSchema2.toSet should contain(StructField("name", StringType, nullable = true))
    }

    test("changing nullability of a column - no-op (true)") {
      val newSchema = df1.setNullableStateOfColumn("name", true).schema
      newSchema shouldBe originalSchema
    }

    test("changing nullability of a column - no-op (false)") {
      val newSchema = df1.setNullableStateOfColumn("language", false).schema
      newSchema shouldBe originalSchema
    }
  }
}
