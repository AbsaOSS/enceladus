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

import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.scalatest.{FunSuite, Matchers}
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}
import za.co.absa.enceladus.utils.udf.UDFLibrary

class StandardizationInterpreter_BinarySuite extends FunSuite with SparkTestBase with LoggerTestBase with Matchers {

  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary

  private val fieldName = "binaryField"

  test("byteArray") {
    val seq = Seq(
      Array(1, 2, 3).map(_.toByte),
      Array('a', 'b', 'c').map(_.toByte)
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, BinaryType, nullable = false)
    ))
    val expected = Seq(
      BinaryRow(Array(1, 2, 3).map(_.toByte)),
      BinaryRow(Array(97, 98, 99).map(_.toByte))
    )

    val src = seq.toDF(fieldName)

    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val result = std.as[BinaryRow].collect().toList

    expected.map(_.simpleFields) should contain theSameElementsAs result.map(_.simpleFields)
  }

}
