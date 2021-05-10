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

package za.co.absa.enceladus.utils.schema

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import za.co.absa.enceladus.utils.testUtils.DataFrameTestUtils.RowSeqToDf

class SparkUtilsSuite extends AnyFunSuite with SparkTestBase with DatasetComparer {

  private def getDummyDataFrame: DataFrame = {
    import spark.implicits._

    Seq(1, 1, 1, 2, 1).toDF("value")
  }

  test("Test setUniqueColumnNameOfCorruptRecord") {
    val expected1 = "_corrupt_record"
    val schema1 = StructType(Seq(StructField("id", LongType)))
    val result1 = SparkUtils.setUniqueColumnNameOfCorruptRecord(spark, schema1)
    assert(result1 == expected1)
    assert(spark.conf.get(SparkUtils.ColumnNameOfCorruptRecordConf) == expected1)
    //two tests in series as the function has side-effects (on provided spark session) and it might collide in parallel run
    val expected2 = "_corrupt_record_1"
    val schema2 = StructType(Seq(StructField("id", LongType), StructField(expected1, BooleanType)))
    val result2 = SparkUtils.setUniqueColumnNameOfCorruptRecord(spark, schema2)
    assert(result2 == expected2)
    assert(spark.conf.get(SparkUtils.ColumnNameOfCorruptRecordConf) == expected2)
  }

  test("Test withColumnIfNotExist() when the column does not exist") {
    val dfIn = getDummyDataFrame
    val dfOut = SparkUtils.withColumnIfDoesNotExist(dfIn, "foo", lit(1))

    // checking schema first
    assert(dfOut.schema.length == 2)
    assert(dfOut.schema.head.name == "value")
    assert(dfOut.schema(1).name == "foo")

    val expectedData = Seq(
      Row(1, 1),
      Row(1, 1),
      Row(1, 1),
      Row(2, 1),
      Row(1, 1)
    )
    val expectedDF = expectedData.toDfWithSchema(dfOut.schema) // checking just the data
    assertSmallDatasetEquality(dfOut, expectedDF)
  }

  test("Test withColumnIfNotExist() when the column exists") {
    val dfIn = getDummyDataFrame
    val dfOut = SparkUtils.withColumnIfDoesNotExist(dfIn, "value", lit(1))

    // checking schema first
    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")

    val expectedData = Seq(
      Row(1, Seq()),
      Row(1, Seq()),
      Row(1, Seq()),
      Row(1, Seq(
        Row("confLitError", "E00005", "Conformance Error - Special column value has changed", "value", Seq("2"), Seq())
      )),
      Row(1, Seq())
    )
    val expectedDF = expectedData.toDfWithSchema(dfOut.schema) // checking just the data
    assertSmallDatasetEquality(dfOut, expectedDF)
  }

  test("Test withColumnIfNotExist() when the column exists, but has a different case") {
    val dfIn = getDummyDataFrame
    val dfOut = SparkUtils.withColumnIfDoesNotExist(dfIn, "vAlUe", lit(1))

    // checking schema first
    assert(dfIn.schema.length == 1)
    assert(dfIn.schema.head.name == "value")

    val expectedData = Seq(
      Row(1, Seq()),
      Row(1, Seq()),
      Row(1, Seq()),
      Row(1, Seq(
        Row("confLitError", "E00005", "Conformance Error - Special column value has changed", "vAlUe", Seq("2"), Seq())
      )),
      Row(1, Seq())
    )
    val expectedDF = expectedData.toDfWithSchema(dfOut.schema) // checking just the data
    assertSmallDatasetEquality(dfOut, expectedDF)  }

}
