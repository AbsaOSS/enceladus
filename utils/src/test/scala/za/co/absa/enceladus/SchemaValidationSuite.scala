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

package za.co.absa.enceladus

import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.validation.SchemaValidator

/**
 * A test suite for validation of scalar data types
 */
//noinspection ZeroIndexToHead
class SchemaValidationSuite extends FunSuite {

  test("Scalar types should be validated") {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("string_good", StringType, nullable = true, Metadata.fromJson(" { \"default\": \"Unknown\" } ")),
        StructField("string_bad", StringType, nullable = true, Metadata.fromJson(" { \"default\": -1 } ")),
        StructField("bool_good", BooleanType, nullable = true, Metadata.fromJson(" { \"default\": \"true\" } ")),
        StructField("bool_bad", BooleanType, nullable = true, Metadata.fromJson(" { \"default\": \"1\" } ")),
        StructField("tiny_good", ByteType, nullable = true, Metadata.fromJson(" { \"default\": \"127\" } ")),
        StructField("tiny_bad", ByteType, nullable = true, Metadata.fromJson(" { \"default\": \"-127-\" } ")),
        StructField("short_good", ShortType, nullable = false, Metadata.fromJson(" { \"default\": \"20000\" } ")),
        StructField("short_bad", ShortType, nullable = false, Metadata.fromJson(" { \"default\": \"20000.0\" } ")),
        StructField("integer_good", IntegerType, nullable = false, Metadata.fromJson(" { \"default\": \"1000000000\" } ")),
        StructField("integer_bad", IntegerType, nullable = false, Metadata.fromJson(" { \"default\": \"number\" } ")),
        StructField("long_good", LongType, nullable = false, Metadata.fromJson(" { \"default\": \"1000000000000000000\" } ")),
        StructField("long_bad", LongType, nullable = false, Metadata.fromJson(" { \"default\": \"wrong\" } ")),
        StructField("float_good", FloatType, nullable = false, Metadata.fromJson(" { \"default\": \"1000.222\" } ")),
        StructField("float_bad", FloatType, nullable = false, Metadata.fromJson(" { \"default\": \"1000.2.22\" } ")),
        StructField("double_good", DoubleType, nullable = false, Metadata.fromJson(" { \"default\": \"1000000.5544\" } ")),
        StructField("double_bad", DoubleType, nullable = false, Metadata.fromJson(" { \"default\": \"++1000000.5544\" } ")),
        StructField("decimal_good", DecimalType(20,10), nullable = false, Metadata.fromJson(" { \"default\": \"314159265.314159265\"}")),
        StructField("decimal_bad", DecimalType(20,10), nullable = false, Metadata.fromJson(" { \"default\": \"314159265358882224.3141.59265\"}"))
      )
    )

    val failures = SchemaValidator.validateSchema(schema)
    if (failures.lengthCompare(9) != 0) {
      println("Validation errors:\n")
      println(failures.mkString("\n"))
    }

    assume(failures.lengthCompare(9) == 0)
    assert(failures(0).fieldName == "string_bad")
    assert(failures(1).fieldName == "bool_bad")
    assert(failures(2).fieldName == "tiny_bad")
    assert(failures(3).fieldName == "short_bad")
    assert(failures(4).fieldName == "integer_bad")
    assert(failures(5).fieldName == "long_bad")
    assert(failures(6).fieldName == "float_bad")
    assert(failures(7).fieldName == "double_bad")
    assert(failures(8).fieldName == "decimal_bad")
  }

  test("Overflows should generate validation errors") {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("tiny_good", ByteType, nullable = true, Metadata.fromJson(" { \"default\": \"127\" } ")),
        StructField("tiny_bad", ByteType, nullable = true, Metadata.fromJson(" { \"default\": \"128\" } ")),
        StructField("short_good", ShortType, nullable = false, Metadata.fromJson(" { \"default\": \"20000\" } ")),
        StructField("short_bad", ShortType, nullable = false, Metadata.fromJson(" { \"default\": \"32768\" } ")),
        StructField("integer_good", IntegerType, nullable = false, Metadata.fromJson(" { \"default\": \"1000000000\" } ")),
        StructField("integer_bad", IntegerType, nullable = false, Metadata.fromJson(" { \"default\": \"6000000000\" } ")),
        StructField("long_good", LongType, nullable = false, Metadata.fromJson(" { \"default\": \"1000000000000000000\" } ")),
        StructField("long_bad", LongType, nullable = false, Metadata.fromJson(" { \"default\": \"10000000000000000000\" } ")),
        StructField("float_good", FloatType, nullable = false, Metadata.fromJson(" { \"default\": \"1000.222\" } ")),
        StructField("float_bad", FloatType, nullable = false, Metadata.fromJson(" { \"default\": \"1e40\" } ")),
        StructField("double_good", DoubleType, nullable = false, Metadata.fromJson(" { \"default\": \"1000000.5544\" } ")),
        StructField("double_bad", DoubleType, nullable = false, Metadata.fromJson(" { \"default\": \"1e310\" } ")),
        StructField("decimal_good", DecimalType(20,10), nullable = false, Metadata.fromJson(" { \"default\": \"314159265351.31415926\"}")),
        StructField("decimal_bad", DecimalType(20,10), nullable = false, Metadata.fromJson(" { \"default\": \"123456789012345678901.12345678901\"}"))
      )
    )

    val failures = SchemaValidator.validateSchema(schema)
    if (failures.lengthCompare(7) != 0) {
      println("Validation errors:\n")
      println(failures.mkString("\n"))
    }

    assume(failures.lengthCompare(7) == 0)
    assert(failures(0).fieldName == "tiny_bad")
    assert(failures(1).fieldName == "short_bad")
    assert(failures(2).fieldName == "integer_bad")
    assert(failures(3).fieldName == "long_bad")
    assert(failures(4).fieldName == "float_bad")
    assert(failures(5).fieldName == "double_bad")
    assert(failures(6).fieldName == "decimal_bad")
  }

  test("Date/Time patterns should be validated") {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("orderdate", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"kk-MM-yyyy\" } ")),
        StructField("deliverydate", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"wrong\" } ")),
        StructField("paymentmade", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"bad\" } ")),
        StructField("paymentreceived", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyyTHH:mm:ss\" } "))
      )
    )

    val failures = SchemaValidator.validateSchema(schema)
    if (failures.lengthCompare(4) != 0) {
      println("Validation errors:\n")
      println(failures.mkString("\n"))
    }

    assume(failures.lengthCompare(4) == 0)
    assert(failures(0).fieldName == "orderdate")
    assert(failures(1).fieldName == "deliverydate")
    assert(failures(2).fieldName == "paymentmade")
    assert(failures(3).fieldName == "paymentreceived")
  }

  test("Date/Time default values should be validated") {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("orderdate", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy\", \"default\": \"2015-01-01\" } ")),
        StructField("deliverydate", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy\", \"default\": \"KKK\" } ")),
        StructField("paymentmade", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy'T'HH:mm:ss\", \"default\": \"2005-01-01T18:00:12\" } ")),
        StructField("paymentreceived", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy'T'HH:mm:ss\", \"default\": \"ZZZ\" } "))
      )
    )

    val failures = SchemaValidator.validateSchema(schema)
    if (failures.lengthCompare(4) != 0) {
      println("Validation errors:\n")
      println(failures.mkString("\n"))
    }

    assume(failures.lengthCompare(4) == 0)
    assert(failures(0).fieldName == "orderdate")
    assert(failures(1).fieldName == "deliverydate")
    assert(failures(2).fieldName == "paymentmade")
    assert(failures(3).fieldName == "paymentreceived")
  }

  test("Nested struct and array fields should be validated") {
    val schema = StructType(
      Array(
        StructField("id", StringType),
        StructField("name", StringType),
        StructField("orders", ArrayType(StructType(Array(
          StructField("orderdate", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"DD-MM-yyyy\" } ")),
          StructField("deliverdate", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy'T'HH:mm:ss\" } ")),
          StructField("ordermade", TimestampType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy'T'HH:mm:ss\" } ")),
          StructField("payment", StructType(Array(
            StructField("due", DateType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy'T'HH:mm:ss\" } ")),
            StructField("made", TimestampType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy\" } ")),
            StructField("expected", TimestampType, nullable = false, Metadata.fromJson(" { \"pattern\": \"foo\" } ")),
            StructField("received", TimestampType, nullable = false, Metadata.fromJson(" { \"pattern\": \"dd-MM-yyyy'T'HH:mm:ss\" } "))))
          ))))
        ),
        StructField("matrix", ArrayType(ArrayType(StructType(Array(
          StructField("bar", TimestampType, nullable = true, Metadata.fromJson(" { \"pattern\": \"DD-MM-yyyy'T'HH:mm:ss\" } ")))))
        ))
      ))
    val failures = SchemaValidator.validateSchema(schema)
    if (failures.lengthCompare(6) != 0) {
      println("Schema:\n")
      println(schema.prettyJson)
      println("Validation errors:\n")
      println(failures.mkString("\n"))
    }

    assume(failures.lengthCompare(6) == 0)
    assert(failures(0).fieldName == "orders[].orderdate")
    assert(failures(1).fieldName == "orders[].deliverdate")
    assert(failures(2).fieldName == "orders[].payment.due")
    assert(failures(3).fieldName == "orders[].payment.made")
    assert(failures(4).fieldName == "orders[].payment.expected")
    assert(failures(5).fieldName == "matrix[][].bar")
  }

  test("Column names should not contain dots") {
    val schema = StructType(
      Array(
        StructField("my.id", StringType),
        StructField("name", StringType),
        StructField("orders", ArrayType(StructType(Array(
          StructField("order.date", StringType),
          StructField("deliverdate", StringType),
          StructField("payment", StructType(Array(
            StructField("due.time", StringType),
            StructField("made", StringType)))
          ))))
        ),
        StructField("matrix", ArrayType(ArrayType(StructType(Array(
          StructField("foo.bar", StringType))))
        ))
      ))
    val failures = SchemaValidator.validateSchema(schema)
    if (failures.lengthCompare(4) != 0) {
      println("Schema:\n")
      println(schema.prettyJson)
      println("Validation errors:\n")
      println(failures.mkString("\n"))
    }

    assume(failures.lengthCompare(4) == 0)
    assert(failures(0).fieldName == "my.id")
    assert(failures(1).fieldName == "orders[].order.date")
    assert(failures(2).fieldName == "orders[].payment.due.time")
    assert(failures(3).fieldName == "matrix[][].foo.bar")
  }

}
