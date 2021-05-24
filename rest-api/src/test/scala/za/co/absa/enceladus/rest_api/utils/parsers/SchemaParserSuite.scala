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

package za.co.absa.enceladus.rest_api.utils.parsers

import org.apache.avro.SchemaParseException
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.mockito.Mockito
import org.scalatest.matchers.should.Matchers
import org.mockito.scalatest.MockitoSugar
import org.scalatest.Inside
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.cobrix.cobol.parser.exceptions.SyntaxErrorException
import za.co.absa.enceladus.restapi.TestResourcePath
import za.co.absa.enceladus.rest_api.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.rest_api.utils.SchemaType
import za.co.absa.enceladus.rest_api.utils.converters.SparkMenasSchemaConvertor

class SchemaParserSuite extends AnyWordSpec with Matchers with MockitoSugar with Inside {
  val mockSchemaConvertor: SparkMenasSchemaConvertor = mock[SparkMenasSchemaConvertor]

  val someStructType: StructType = StructType(Seq(StructField(name = "field1", dataType = DataTypes.IntegerType)))
  Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenReturn(someStructType)

  private def readTestResourceAsString(path: String) = IOUtils.toString(getClass.getResourceAsStream(path))

  private def readTestResourceAsDataType(path: String) = DataType.fromJson(readTestResourceAsString(path)).asInstanceOf[StructType]

  import SchemaType._

  val schemaParserFactory: SchemaParser.SchemaParserFactory = SchemaParser.getFactory(mockSchemaConvertor) // SUT
  "SchemaParser" should {

    val structParser = schemaParserFactory.getParser(Struct)
    "parse struct correctly" when {
      "menasSchemaConverter suceeds" in {
        structParser.parse("some struct type def") shouldBe someStructType
      }
    }

    "throw SchemaParsingException at parse struct " when {
      "menasSchemaConverter throws exception" in {
        val someException = new IllegalStateException("error description")
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenThrow(someException)

        val caughtException = the[SchemaParsingException] thrownBy {
          structParser.parse("bad struct type def")
        }
        caughtException shouldBe SchemaParsingException(SchemaType.Struct, someException.getMessage, cause = someException)
      }
    }


    val avroParser = schemaParserFactory.getParser(Avro)
    "parse avro schema to StructType" when {
      "correct avsc file content is given" in {
        val expectedStructType = readTestResourceAsDataType(TestResourcePath.Avro.okJsonEquivalent)

        val schemaContent = readTestResourceAsString(TestResourcePath.Avro.ok)
        avroParser.parse(schemaContent) shouldBe expectedStructType
      }
    }

    "throw SchemaParsingException at parse avro" when {
      "given unparsable avsc content" in {
        val caughtException = the[SchemaParsingException] thrownBy {
          avroParser.parse("invalid avsc")
        }

        inside(caughtException) { case SchemaParsingException(SchemaType.Avro, msg, _, _, _, cause) =>
          msg should include("expected a valid value")
          cause shouldBe a[SchemaParseException]
        }
      }
    }

    val copybookParser = schemaParserFactory.getParser(Copybook)
    "parse cobol copybook schema to StructType" when {
      "correct cob file content is given" in {
        val expectedCopybookType = readTestResourceAsDataType(TestResourcePath.Copybook.okJsonEquivalent)

        val schemaContent = readTestResourceAsString(TestResourcePath.Copybook.ok)
        copybookParser.parse(schemaContent) shouldBe expectedCopybookType
      }
    }

    "throw SchemaParsingException at parse copybook" when {
      "given unparsable cob content" in {
        val schemaContent = readTestResourceAsString(TestResourcePath.Copybook.bogus)

        val caughtException = the[SchemaParsingException] thrownBy {
          copybookParser.parse(schemaContent)
        }

        inside(caughtException) {
          case SchemaParsingException(SchemaType.Copybook, msg, Some(22), None, Some(""), cause) =>
            msg should include("Syntax error in the copybook")
            cause shouldBe a[SyntaxErrorException]
        }
      }
    }

  }
}
