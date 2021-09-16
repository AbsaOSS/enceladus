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

import org.apache.spark.sql.types.{BinaryType, Metadata, MetadataBuilder, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.testUtils.{LoggerTestBase, SparkTestBase}
import za.co.absa.enceladus.utils.types.{Defaults, GlobalDefaults}
import za.co.absa.enceladus.utils.udf.UDFLibrary
import za.co.absa.enceladus.utils.validation.ValidationException

class StandardizationInterpreter_BinarySuite extends AnyFunSuite with SparkTestBase with LoggerTestBase with Matchers {

  import spark.implicits._

  private implicit val udfLib: UDFLibrary = new UDFLibrary
  private implicit val defaults: Defaults = GlobalDefaults

  private val fieldName = "binaryField"

  test("byteArray to Binary") {
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

  test("Binary from string with base64 encoding") {
    val seq = Seq(
      "MTIz",
      "YWJjZA==",
      "bogus#$%^" // invalid base64 chars
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, BinaryType, nullable = false,
        new MetadataBuilder().putString("encoding", "base64").build)
    ))

    val expected = Seq(
      BinaryRow(Array(49, 50, 51).map(_.toByte)), // "123"
      BinaryRow(Array(97, 98, 99, 100).map(_.toByte)), // "abcd"
      BinaryRow(Array.emptyByteArray, // default value on error
        Seq(ErrorMessage("stdCastError", "E00000", "Standardization Error - Type cast", "binaryField",
          rawValues = Seq("bogus#$%^"), mappings = Seq()))
      )
    )

    val src = seq.toDF(fieldName)
    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val result = std.as[BinaryRow].collect().toList
    expected.map(_.simpleFields) should contain theSameElementsAs result.map(_.simpleFields)
  }

  test("Binary from string with bogus encoding") {
    val seq = Seq(
      "does not matter"
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, BinaryType, nullable = false,
        new MetadataBuilder().putString("encoding", "bogus").build)
    ))

    val src = seq.toDF(fieldName)
    val caught = intercept[ValidationException](
      StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    )

    caught.errors.length shouldBe 1
    caught.errors.head should include("Unsupported encoding for Binary field binaryField: 'bogus'")
  }

  // behavior of explicit metadata "none" and lacking metadata should behave identically
  Seq(None, Some("none")).foreach { enc =>
    test(s"Binary from string with ${enc.getOrElse("missing")} encoding") {
      val seq = Seq(
        "abc",
        "1234"
      )

      val metadata = enc.fold(Metadata.empty)(e => new MetadataBuilder().putString("encoding", e).build)
      val desiredSchema = StructType(Seq(
        StructField(fieldName, BinaryType, nullable = false, metadata)
      ))

      val expected = Seq(
        BinaryRow(Array(97, 98, 99).map(_.toByte)), // "123"
        BinaryRow(Array(49, 50, 51, 52).map(_.toByte)) // "abcd"
      )

      val src = seq.toDF(fieldName)
      val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
      logDataFrameContent(std)

      val result = std.as[BinaryRow].collect().toList
      expected.map(_.simpleFields) should contain theSameElementsAs result.map(_.simpleFields)
    }
  }

  test("Binary with defaultValue uses base64") {
    val seq = Seq[Option[String]](
      Some("MTIz"),
      None
    )
    val desiredSchema = StructType(Seq(
      StructField(fieldName, BinaryType, nullable = false, new MetadataBuilder()
        .putString("encoding", "base64")
        .putString("default", "ZW1wdHk=") // "empty"
        .build)
    ))

    val expected = Seq(
      BinaryRow(Array(49, 50, 51).map(_.toByte)), // "123"
      // ^ std error is written into the errCol and the default (fallback) value "(binary) empty" is used.
      BinaryRow(Array('e', 'm', 'p', 't', 'y').map(_.toByte),
        Seq(ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute",
          "binaryField", rawValues = Seq("null"), mappings = Seq()))
      )
    )

    val src = seq.toDF(fieldName)
    val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
    logDataFrameContent(std)

    val result = std.as[BinaryRow].collect().toList
    expected.map(_.simpleFields) should contain theSameElementsAs result.map(_.simpleFields)
  }

  // behavior of explicit metadata "none" and lacking metadata should behave identically
  Seq(None, Some("none")).foreach { enc =>
    test(s"Binary with defaultValue ${enc.getOrElse("missing")} encoding") {
      val seq = Seq[Option[String]](
        Some("123"),
        None
      )

      val metadata = {
        val base = new MetadataBuilder().putString("default", "fallback1")
        enc.fold(base)(base.putString("encoding", _))
        base.build
      }

      val desiredSchema = StructType(Seq(StructField(fieldName, BinaryType, nullable = false, metadata)))

      val expected = Seq(
        BinaryRow(Array(49, 50, 51).map(_.toByte)), // "123"
        // ^ std error is written into the errCol and the default (fallback) value "(binary) empty" is used.
        BinaryRow(Array('f', 'a', 'l', 'l', 'b', 'a', 'c', 'k', '1').map(_.toByte),
          Seq(ErrorMessage("stdNullError", "E00002", "Standardization Error - Null detected in non-nullable attribute",
            "binaryField", rawValues = Seq("null"), mappings = Seq()))
        )
      )

      val src = seq.toDF(fieldName)
      val std = StandardizationInterpreter.standardize(src, desiredSchema, "").cache()
      logDataFrameContent(std)

      val result = std.as[BinaryRow].collect().toList
      expected.map(_.simpleFields) should contain theSameElementsAs result.map(_.simpleFields)
    }
  }

}
