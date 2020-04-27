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

package za.co.absa.enceladus

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.validation.{SchemaPathValidator, ValidationError, ValidationIssue, ValidationWarning}

/**
  * A test suite for validation of schema path fields existence.
  */
class SchemaPathValidatorSuite extends FunSuite {

  private val schema = StructType(
    Array(
      StructField("id", StringType),
      StructField("person", StructType(Array(
        StructField("firstName", DateType),
        StructField("lastName", DateType)))),
      StructField("orders", ArrayType(StructType(Array(
        StructField("orderdate", DateType),
        StructField("deliverdate", DateType),
        StructField("ordermade", TimestampType),
        StructField("payment", StructType(Array(
          StructField("due", DateType),
          StructField("made", TimestampType),
          StructField("expected", TimestampType),
          StructField("received", TimestampType)))
        ))))
      ),
      StructField("matrix", ArrayType(ArrayType(StructType(Array(
        StructField("bar", TimestampType))))
      ))
    ))

  test("Validator should be able to detect column names that does not exist") {

    // These should pass the validation
    assert(SchemaPathValidator.validateSchemaPath(schema, "id").isEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "person.firstName").isEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "orders.orderdate").isEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "orders.deliverdate").isEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "orders.payment.due").isEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "matrix.bar").isEmpty)

    assert(SchemaPathValidator.validateSchemaPathParent(schema, "matrix.something").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathParent(schema, "person.P2").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathParent(schema, "orders.new_order").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathParent(schema, "orders.payment.NEWFIELD").isEmpty)

    // These should not pass the validation
    assert(SchemaPathValidator.validateSchemaPath(schema, "ID").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "person.FN").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "orders.OrderDate").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "orders.deliver_date").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "orders.payment.DUE").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPath(schema, "matrix.bar12").nonEmpty)

    // tagret attribute should not exist
    //assert(SchemaPathValidator.validateSchemaPathParent("table", schema, "person.firstName").nonEmpty)

    assert(SchemaPathValidator.validateSchemaPathParent(schema, "matrix.bar.something").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathParent(schema, "person.firstName.val").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathParent(schema, "orders.orderdate.extra").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathParent(schema, "orders.payment.made.metadata").nonEmpty)

    val issues1 = SchemaPathValidator.validateSchemaPath(schema, "person.firstname")
    assert(issues1.nonEmpty)
    val msg1 = extractMag(issues1.head)
    assert(msg1.contains("Column name 'person.firstname' does not case-sensitively match 'person.firstName"))

    val issues2 = SchemaPathValidator.validateSchemaPath(schema, "orders.orderDate")
    assert(issues2.nonEmpty)
    val msg2 = extractMag(issues2.head)
    assert(msg2.contains("Column name 'orders.orderDate' does not case-sensitively match 'orders.orderdate"))

    val issues3 = SchemaPathValidator.validateSchemaPath(schema, "orders.unknowncolumn")
    assert(issues3.nonEmpty)
    val msg3 = extractMag(issues3.head)
    assert(msg3.contains("Column name 'orders.unknowncolumn' does not exist"))
  }

  test("Validator should be able to detect non-primitive types") {
    // These should pass the validation
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "id").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "person.firstName").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "orders.orderdate").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "orders.payment.due").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "matrix.bar").isEmpty)

    // These should not pass the validation
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "Person.FirstName").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "person").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "orders").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "matrix").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathPrimitive(schema, "").nonEmpty)
  }

  test("Validator should be able to detect non-numeric types") {
    val typeSchema = StructType(
      Array(
        StructField("string", StringType),
        StructField("integer", IntegerType),
        StructField("struct", StructType(Array(
          StructField("date", DateType),
          StructField("timestamp", TimestampType)))),
        StructField("array", ArrayType(StructType(Array(
          StructField("binary", BinaryType),
          StructField("boolean", BooleanType),
          StructField("calencarInterval", CalendarIntervalType),
          StructField("null", NullType),
          StructField("byte", ByteType),
          StructField("decimal", DecimalType(20, 10)),
          StructField("double", DoubleType),
          StructField("float", FloatType),
          StructField("integer", IntegerType),
          StructField("long", LongType),
          StructField("short", ShortType),
          StructField("struct", StructType(Array(
            StructField("date", DateType),
            StructField("timestamp", TimestampType),
            StructField("string", StringType),
            StructField("integer", IntegerType)))
          ))))
        ),
        StructField("matrix", ArrayType(ArrayType(StructType(Array(
          StructField("bar", TimestampType))))
        ))
      ))

    // These should pass the validation
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "integer").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.byte").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.decimal").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.double").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.float").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.integer").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.long").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.short").isEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.struct.integer").isEmpty)

    // These should not pass the validation
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "Person.FirstName").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "string").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "struct").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "struct.date").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "struct.timestamp").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.binary").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.boolean").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.calendarInterval").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.null").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.struct.date").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.struct.timestamp").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "array.struct.string").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "matrix").nonEmpty)
    assert(SchemaPathValidator.validateSchemaPathNumeric(typeSchema, "").nonEmpty)
  }

  test("Validator should be able to detect if fields have different parents") {
    // These should pass the validation
    assert(SchemaPathValidator.validatePathSameParent("id", "person").isEmpty)

    // These should not pass the validation
    assert(SchemaPathValidator.validatePathSameParent("id", "person.fullname").nonEmpty)
  }

  private def extractMag(issue: ValidationIssue): String = {
    issue match {
      case ValidationWarning(msg) => msg
      case ValidationError(msg) => msg
    }
  }

}
