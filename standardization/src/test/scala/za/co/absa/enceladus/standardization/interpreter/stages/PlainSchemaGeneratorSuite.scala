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

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class PlainSchemaGeneratorSuite extends FunSuite with SparkTestBase {
  val schema = StructType(Seq(
    StructField("a", IntegerType, nullable = false),
    StructField("b", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build),
    StructField("c", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_c").build),
    StructField("d", ArrayType(StructType(Seq(
      StructField("e", StructType(Seq(
        StructField("f", ArrayType(StructType(Seq(
          StructField("g", IntegerType, nullable = false),
          StructField("h", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build),
          StructField("i", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_i").build)
        ))))
      )))
    ))))
  ))

  val expectedSchema = StructType(Seq(
    StructField("a", StringType, nullable = true),
    StructField("b", StringType, nullable = true, new MetadataBuilder().putString("meta", "data").build),
    StructField("override_c", StringType, nullable = true, new MetadataBuilder().putString("sourcecolumn", "override_c").build),
    StructField("d", ArrayType(StructType(Seq(
      StructField("e", StructType(Seq(
        StructField("f", ArrayType(StructType(Seq(
          StructField("g", StringType, nullable = true),
          StructField("h", StringType, nullable = true, new MetadataBuilder().putString("meta", "data").build),
          StructField("override_i", StringType, nullable = true, new MetadataBuilder().putString("sourcecolumn", "override_i").build)
        ))))
      )))
    ))))
  ))

  test("Test generateInputSchema") {
    val generatedSchema = PlainSchemaGenerator.generateInputSchema(schema)
    assertResult(expectedSchema)(generatedSchema)
  }

}
