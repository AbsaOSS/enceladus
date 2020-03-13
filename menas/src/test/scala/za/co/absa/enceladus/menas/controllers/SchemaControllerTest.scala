/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.enceladus.menas.controllers

import org.apache.avro.SchemaParseException
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Inside, Matchers, WordSpec}
import za.co.absa.enceladus.menas.controllers.SchemaController._
import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor

import scala.io.Source

class SchemaControllerTest extends WordSpec with Matchers with MockitoSugar with Inside {

  val mockSchemaConvertor: SparkMenasSchemaConvertor = mock[SparkMenasSchemaConvertor]
  val schemaController = new SchemaController(null, null, mockSchemaConvertor) // SUT

  "SchemaController" should {
    "parseStructType correctly" when {
      "menasSchemaConverter suceeds" in {
        val someStructType: StructType = StructType(Seq(StructField(name = "field1", dataType = DataTypes.IntegerType)))
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenReturn(someStructType)
        schemaController.parseStructType("some struct type def") shouldBe someStructType
      }
    }

    "throw SchemaParsingException at parseStructType " when {
      "menasSchemaConverter throws exception" in {
        val someException = new IllegalStateException("error description")
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenThrow(someException)

        val caughtException = the[SchemaParsingException] thrownBy {
          schemaController.parseStructType("bad struct type def")
        }
        caughtException shouldBe SchemaParsingException(SchemaTypeStruct, someException.getMessage, cause = someException)
      }
    }
  }

  "parse avro schema to StructType" when {
    "correct avsc file content is given" in {
      val expectedJsonFormatSchema = Source.fromFile("src/test/resources/test_data/schemas/avro/equivalent-to-avroschema.json").mkString
      val expectedStructType = DataType.fromJson(expectedJsonFormatSchema).asInstanceOf[StructType]

      val schemaContent = Source.fromFile("src/test/resources/test_data/schemas/avro/avroschema_json_ok.avsc").mkString

      schemaController.parseAvro(schemaContent) shouldBe expectedStructType
    }
  }

  "throw SchemaParsingException at parseAvro" when {
    "given unparsable avsc content" in {
      val caughtException = the[SchemaParsingException] thrownBy {
        schemaController.parseAvro("invalid avsc")
      }

      inside (caughtException) { case SchemaParsingException(SchemaTypeAvro, msg, _, _, _, cause) =>
        msg should include ("expected a valid value")
        cause shouldBe a [SchemaParseException]
      }
    }
  }

}
