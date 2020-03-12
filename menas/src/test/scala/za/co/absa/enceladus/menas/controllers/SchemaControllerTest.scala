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

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import za.co.absa.enceladus.menas.controllers.SchemaController.SchemaTypeStruct
import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor

class SchemaControllerTest extends WordSpec with Matchers with MockitoSugar {

  val mockSchemaConvertor: SparkMenasSchemaConvertor = mock[SparkMenasSchemaConvertor]
  val someStructType: StructType = StructType(Seq(StructField(name = "field1", dataType = DataTypes.IntegerType)))

  "SchemaController" should {
    "parseStructType correctly" when {
      "menasSchemaConverter suceeds" in {
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenReturn(someStructType)

        (new SchemaController(null, null, mockSchemaConvertor)
          .parseStructType("some struct type def")) shouldBe someStructType
      }
    }

    "throw SchemaParsingException at parseStructType " when {
      "menasSchemaConverter throws exception" in {
        val someException = new IllegalStateException("error description")
        Mockito.when(mockSchemaConvertor.convertAnyToStructType(any[String])).thenThrow(someException)

        val caughtException = the[SchemaParsingException] thrownBy {
          new SchemaController(null, null, mockSchemaConvertor).parseStructType("bad struct type def")
        }
        caughtException shouldBe SchemaParsingException(SchemaTypeStruct, someException.getMessage, cause = someException)
      }
    }
  }
}
