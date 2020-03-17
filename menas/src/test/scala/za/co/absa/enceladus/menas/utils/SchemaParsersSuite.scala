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

package za.co.absa.enceladus.menas.utils

import org.apache.avro.SchemaParseException
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Inside, Matchers, WordSpec}
import za.co.absa.cobrix.cobol.parser.exceptions.SyntaxErrorException
import za.co.absa.enceladus.menas.controllers.SchemaType
import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor

class SchemaParsersSuite extends WordSpec with Matchers with MockitoSugar with Inside {
  val mockSchemaConvertor: SparkMenasSchemaConvertor = mock[SparkMenasSchemaConvertor]
  val schemaParsers = new SchemaParsers(mockSchemaConvertor) // SUT

  private def readTestResourceAsString(path: String) = IOUtils.toString(getClass.getResourceAsStream(path))

  private def readTestResourceAsDataType(path: String) = DataType.fromJson(readTestResourceAsString(path)).asInstanceOf[StructType]

  "SchemaParsers" should {
    "parseStructType correctly" when {
      "menasSchemaConverter suceeds" in {
        val someStructType: StructType = StructType(Seq(StructField(name = "field1", dataType = DataTypes.IntegerType)))
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenReturn(someStructType)
        schemaParsers.parseStructType("some struct type def") shouldBe someStructType
      }
    }

    "throw SchemaParsingException at parseStructType " when {
      "menasSchemaConverter throws exception" in {
        val someException = new IllegalStateException("error description")
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenThrow(someException)

        val caughtException = the[SchemaParsingException] thrownBy {
          schemaParsers.parseStructType("bad struct type def")
        }
        caughtException shouldBe SchemaParsingException(SchemaType.Struct, someException.getMessage, cause = someException)
      }
    }
  }

  "parse avro schema to StructType" when {
    "correct avsc file content is given" in {
      val expectedStructType = readTestResourceAsDataType("/test_data/schemas/avro/equivalent-to-avroschema.json")

      val schemaContent = readTestResourceAsString("/test_data/schemas/avro/avroschema_json_ok.avsc")
      schemaParsers.parseAvro(schemaContent) shouldBe expectedStructType
    }
  }

  "throw SchemaParsingException at parseAvro" when {
    "given unparsable avsc content" in {
      val caughtException = the[SchemaParsingException] thrownBy {
        schemaParsers.parseAvro("invalid avsc")
      }

      inside(caughtException) { case SchemaParsingException(SchemaType.Avro, msg, _, _, _, cause) =>
        msg should include("expected a valid value")
        cause shouldBe a[SchemaParseException]
      }
    }
  }

  "parse cobol copybook schema to StructType" when {
    "correct cob file content is given" in {
      val expectedCopybookType = readTestResourceAsDataType("/test_data/schemas/copybook/equivalent-to-copybook.json")

      val schemaContent = readTestResourceAsString("/test_data/schemas/copybook/copybook_ok.cob")
      schemaParsers.parseCopybook(schemaContent) shouldBe expectedCopybookType
    }
  }

  "throw SchemaParsingException at parseCopybook" when {
    "given unparsable cob content" in {
      val schemaContent = readTestResourceAsString("/test_data/schemas/copybook/copybook_bogus.cob")

      val caughtException = the[SchemaParsingException] thrownBy {
        schemaParsers.parseCopybook(schemaContent)
      }

      inside(caughtException) {
        case SchemaParsingException(SchemaType.Copybook, msg, Some(22), None, Some("B1"), cause) =>
          msg should include("Syntax error in the copybook")
          cause shouldBe a[SyntaxErrorException]
      }
    }
  }

}
