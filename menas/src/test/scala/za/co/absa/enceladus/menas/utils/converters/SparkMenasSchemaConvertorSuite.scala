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

package za.co.absa.enceladus.menas.utils.converters

import org.apache.spark.sql.types._
import za.co.absa.enceladus.model._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.databind.SerializationFeature
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.menas.models.rest.exceptions.SchemaParsingException

class SparkMenasSchemaConvertorSuite extends AnyFunSuite with SparkTestBase {
  private val objectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
  private val sparkConvertor = new SparkMenasSchemaConvertor(objectMapper)

  private val sparkSimleFlat = Seq(
    StructField(name = "a", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("format", "xyz.abc").putString("precision", "14.56").build),
    StructField(name = "b", dataType = DecimalType.apply(38, 18), nullable = false),
    StructField(name = "c", dataType = StringType))

  private val menasSimpleFlat = Seq(
    SchemaField(name = "a", `type` = "integer", path = "", elementType = None, containsNull = None, nullable = true, metadata = Map("format" -> "xyz.abc", "precision" -> "14.56"), children = List()),
    SchemaField(name = "b", `type` = "decimal(38,18)", path = "", elementType = None, containsNull = None, nullable = false, metadata = Map(), children = List()),
    SchemaField(name = "c", `type` = "string", path = "", elementType = None, containsNull = None, nullable = true, metadata = Map(), children = List()))

  test("convertSparkToMenasFields Simple Test") {
    val res = sparkConvertor.convertSparkToMenasFields(sparkSimleFlat)

    assertResult(menasSimpleFlat)(res)

    assertResult(sparkConvertor.convertSparkToMenasFields(Seq[StructField]()))(Seq[SchemaField]())
  }

  test("convertSparkToMenas Complex Test") {
    val sparkComplex = Seq(
      StructField(name = "a", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("format", "xyz.abc").putString("precision", "14.56").build),
      StructField(name = "b", dataType = StructType(Seq(
        StructField(name = "c", dataType = ArrayType.apply(IntegerType)),
        StructField(name = "d", dataType = ArrayType.apply(StructType(Seq(
          StructField(name = "e", dataType = StringType),
          StructField(name = "f", dataType = DoubleType))))),
        StructField(name = "g", dataType = ArrayType.apply(ArrayType.apply(StructType(Seq(
          StructField(name = "h", dataType = IntegerType)))))))), nullable = false))

    val menasComplex = Seq(
      SchemaField(name = "a", `type` = "integer", path = "", elementType = None, containsNull = None, nullable = true, metadata = Map("format" -> "xyz.abc", "precision" -> "14.56"), children = List()),
      SchemaField(name = "b", `type` = "struct", path = "", elementType = None, containsNull = None, nullable = false, metadata = Map(), children = List(
        SchemaField(name = "c", `type` = "array", path = "b", elementType = Some("integer"), containsNull = Some(true), nullable = true, metadata = Map(), children = List()),
        SchemaField(name = "d", `type` = "array", path = "b", elementType = Some("struct"), containsNull = Some(true), nullable = true, metadata = Map(), children = List(
          SchemaField(name = "e", `type` = "string", path = "b.d", elementType = None, containsNull = None, nullable = true, metadata = Map(), children = List()),
          SchemaField(name = "f", `type` = "double", path = "b.d", elementType = None, containsNull = None, nullable = true, metadata = Map(), children = List()))),
        SchemaField(name = "g", `type` = "array", path = "b", elementType = Some("array"), containsNull = Some(true), nullable = true, metadata = Map(), children = List(
          SchemaField(name = "", `type` = "array", path = "b.g", elementType = Some("struct"), containsNull = Some(true), nullable = true, metadata = Map(), children = List(
            SchemaField(name = "h", `type` = "integer", path = "b.g", elementType = None, containsNull = None, nullable = true, metadata = Map(), children = List()))))))))

    val res = sparkConvertor.convertSparkToMenasFields(sparkComplex)

    assertResult(menasComplex)(res)

    assertResult(sparkConvertor.convertMenasToSparkFields(menasComplex))(sparkComplex)
  }

  test("convertMenasToSpark Simple Test") {
    val res = sparkConvertor.convertMenasToSparkFields(menasSimpleFlat)

    assertResult(sparkSimleFlat)(res)
  }

  test("convertSparkToMenasFields with non-string values in metadata") {
    val fieldName = "field_name"
    val sparkDefinition = Seq(
      StructField(name = fieldName, dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putLong("default", 0).build)
    )

    val caught = intercept[SchemaParsingException] {
      sparkConvertor.convertSparkToMenasFields(sparkDefinition)
    }

    assert(caught == SchemaParsingException(schemaType = null, message = "Value for metadata key 'default' (of value 0) to be a string or null", field = Option(fieldName))) // scalastyle:ignore null
  }

  test("convertSparkToMenasFields and convertMenasToSparkFields with nulls in values of metadata") {
    val fieldName = "field_with_null_metadata_values"
    val sparkDefinition = Seq(
      StructField(name = fieldName, dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putNull("default").putString("foo", "bar").build)
    )
    val menasDefinition = Seq(
      SchemaField(name = fieldName, `type` = "integer", path = "", elementType = None, containsNull = None, nullable = true, metadata = Map("default" -> null, "foo" -> "bar"), children = List()) // scalastyle:ignore null
    )

    val res1 = sparkConvertor.convertSparkToMenasFields(sparkDefinition)
    assertResult(menasDefinition)(res1)

    val res2 = sparkConvertor.convertMenasToSparkFields(menasDefinition)
    assertResult(sparkDefinition)(res2)
  }

}
