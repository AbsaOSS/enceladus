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

package za.co.absa.enceladus.standardization.interpreter

import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, MetadataBuilder, StringType, StructField, StructType}
import org.scalatest.FunSuite
import za.co.absa.enceladus.standardization.interpreter.StandardizationInterpreterSuite._
import za.co.absa.enceladus.utils.error.{ErrorMessage, UDFLibrary}
import za.co.absa.enceladus.utils.fs.FileReader
import za.co.absa.enceladus.utils.general.JsonUtils
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}

class StandardizationInterpreterSuite  extends FunSuite with SparkTestBase with LoggerTestBase {
  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

  test("sourcecolumn meta (does not yet) rename field") {
    /* TODO once ##398 is done and solved as expected this test should start to fail
       (which is good - see the two columns of same name on the output) */
    val sourceColumnName = "source column"
    val desiredSchema = StructType(Seq(
      StructField("description", StringType, nullable = false),
      StructField("new_column", StringType, nullable = false,
        new MetadataBuilder().putString("sourcecolumn", sourceColumnName).build),
      StructField("new_column_nullable", StringType, nullable = true,
        new MetadataBuilder().putString("sourcecolumn", sourceColumnName).build)
    ))

    val seq = Seq(
      ("01-Hello", "World"),
      ("02-Null", null),
      ("03-Number", Long.MaxValue.toString)
    )
    val src = seq.toDF("description", sourceColumnName)
    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val actualSchema = std.schema.treeString
    val expectedSchema = "root\n" +
      " |-- description: string (nullable = true)\n" +
      " |-- source column: string (nullable = true)\n" +
      " |-- source column: string (nullable = true)\n" +
      " |-- errCol: array (nullable = true)\n" +
      " |    |-- element: struct (containsNull = false)\n" +
      " |    |    |-- errType: string (nullable = true)\n" +
      " |    |    |-- errCode: string (nullable = true)\n" +
      " |    |    |-- errMsg: string (nullable = true)\n" +
      " |    |    |-- errCol: string (nullable = true)\n" +
      " |    |    |-- rawValues: array (nullable = true)\n" +
      " |    |    |    |-- element: string (containsNull = true)\n" +
      " |    |    |-- mappings: array (nullable = true)\n" +
      " |    |    |    |-- element: struct (containsNull = true)\n" +
      " |    |    |    |    |-- mappingTableColumn: string (nullable = true)\n" +
      " |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)\n"
    assert(actualSchema == expectedSchema)

    val exp = Seq(
      RenamingRow("01-Hello", Option("World")),
      RenamingRow("02-Null", None, Seq(
        ErrorMessage.stdNullErr(sourceColumnName)
      )),
      RenamingRow("03-Number", Option(Long.MaxValue.toString))
    )

    assertResult(exp)(std.as[RenamingRow].collect().toList)
  }

  test("Errors in fields and having source columns") {
    val desiredSchema = StructType(Seq(
      StructField("first_name", StringType, nullable = true,
        new MetadataBuilder().putString("sourcecolumn", "first name").build),
      StructField("last_name", StringType, nullable = false,
        new MetadataBuilder().putString("sourcecolumn", "last name").build),
      StructField("body_stats",
        StructType(Seq(
          StructField("height", IntegerType, nullable = false),
          StructField("weight", IntegerType, nullable = false),
          StructField("miscellaneous", StructType(Seq(
           StructField("eye_color", StringType, nullable = true,
             new MetadataBuilder().putString("sourcecolumn", "eye color").build),
           StructField("glasses", BooleanType, nullable = true)
          ))),
          StructField("temperature_measurements", ArrayType(DoubleType, containsNull = false), nullable = false,
            new MetadataBuilder().putString("sourcecolumn", "temperature measurements").build)
        )),
        nullable = false,
        new MetadataBuilder().putString("sourcecolumn", "body stats").build
      )
    ))


    val srcString:String = FileReader.readFileAsString("src/test/resources/data/patients.json")

    val src = JsonUtils.getDataFrameFromJson(spark, Seq(srcString))

    logDataFrameContent(src)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val actualSchema = std.schema.treeString
    val expectedSchema = "root\n" +
                         " |-- first name: string (nullable = true)\n" +
                         " |-- last name: string (nullable = true)\n" +
                         " |-- body stats: struct (nullable = false)\n" +
                         " |    |-- height: integer (nullable = true)\n" +
                         " |    |-- weight: integer (nullable = true)\n" +
                         " |    |-- miscellaneous: struct (nullable = false)\n" +
                         " |    |    |-- eye_color: string (nullable = true)\n" +
                          // TODO #398 notice that this field is renamed (not renamed back)
                         " |    |    |-- glasses: boolean (nullable = true)\n" +
                         " |    |-- temperature measurements: array (nullable = true)\n" +
                          // TODO after #398 should be 'temperature measurements' (among other changes)
                         " |    |    |-- element: double (containsNull = true)\n" +
                         " |-- errCol: array (nullable = true)\n" +
                         " |    |-- element: struct (containsNull = false)\n" +
                         " |    |    |-- errType: string (nullable = true)\n" +
                         " |    |    |-- errCode: string (nullable = true)\n" +
                         " |    |    |-- errMsg: string (nullable = true)\n" +
                         " |    |    |-- errCol: string (nullable = true)\n" +
                         " |    |    |-- rawValues: array (nullable = true)\n" +
                         " |    |    |    |-- element: string (containsNull = true)\n" +
                         " |    |    |-- mappings: array (nullable = true)\n" +
                         " |    |    |    |-- element: struct (containsNull = true)\n" +
                         " |    |    |    |    |-- mappingTableColumn: string (nullable = true)\n" +
                         " |    |    |    |    |-- mappedDatasetColumn: string (nullable = true)\n"
    assert(actualSchema == expectedSchema)

    val exp = Seq(
      PatientRow("Jane", "Goodall", BodyStats(164, 61, "green", Option(true), Seq(36.6, 36.7, 37.0, 36.6))),
      PatientRow("Scott", "Lang", BodyStats(0, 83, "blue", Option(false),Seq(36.6, 36.7, 37.0, 36.6)), Seq(
        ErrorMessage.stdCastErr("body stats.height", "various")
      )),
      PatientRow("Aldrich", "Killian", BodyStats(181, 90, "brown or orange", None, Seq(36.7, 36.5, 38.0, 48.0, 152.0, 831.0, 0.0)), Seq(
        ErrorMessage.stdCastErr("body stats.miscellaneous.glasses", "not any more"),
        ErrorMessage.stdCastErr("body stats.temperature measurements[*]", "exploded")
      ))
    )

    assertResult(exp)(std.as[PatientRow].collect().toList)
  }
}

object StandardizationInterpreterSuite {
  // cannot use case class as the field names contain spaces therefore cast will happen into tuple
  type RenamingRow = (String, String, Option[String], Seq[ErrorMessage])

  type BodyStats = (Int, Int, (String, Option[Boolean]), Seq[Double])
  type PatientRow = (String, String, BodyStats, Seq[ErrorMessage])

  object RenamingRow {
    def apply(
               description: String,
               sourceColumn: Option[String],
               errCol: Seq[ErrorMessage] = Seq.empty
             ): RenamingRow = {
      (description, sourceColumn.getOrElse(""), sourceColumn, errCol)
    }

  }

  object BodyStats {
    def apply(
               height: Int,
               weight: Int,
               eyeColor: String,
               glasses: Option[Boolean],
               temperatureMeasurements: Seq[Double]
             ): BodyStats = {
      (height, weight, (eyeColor, glasses), temperatureMeasurements)
    }
  }

  object PatientRow {
    def apply(
               first_name: String,
               lastName: String,
               bodyStats: BodyStats,
               errCol: Seq[ErrorMessage] = Seq.empty
             ): PatientRow = {
      (first_name, lastName, bodyStats, errCol)
    }
  }
}
