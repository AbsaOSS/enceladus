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

package za.co.absa.enceladus.utils.schema

import org.scalatest.FunSuite
import org.apache.spark.sql.types._
import za.co.absa.enceladus.utils.schema.SchemaUtils._

class SchemaUtilsSuite extends FunSuite {
  // scalastyle:off magic.number

  val schema = StructType(Seq(
    StructField("a", IntegerType, false),
    StructField("b", StructType(Seq(
      StructField("c", IntegerType),
      StructField("d", StructType(Seq(
        StructField("e", IntegerType))), true)))),
    StructField("f", StructType(Seq(
      StructField("g", ArrayType.apply(StructType(Seq(
        StructField("h", IntegerType))))))))))

  val nestedSchema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", ArrayType(StructType(Seq(
      StructField("c", StructType(Seq(
        StructField("d", ArrayType(StructType(Seq(
          StructField("e", IntegerType))))))))))))))

  val structFieldNoMetadata = StructField("a", IntegerType)

  val structFieldWithMetadataNotSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
  val structFieldWithMetadataSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_a").build)

  test("Testing getFieldType") {

    val a = getFieldType("a", schema)
    val b = getFieldType("b", schema)
    val c = getFieldType("b.c", schema)
    val d = getFieldType("b.d", schema)
    val e = getFieldType("b.d.e", schema)
    val f = getFieldType("f", schema)
    val g = getFieldType("f.g", schema)
    val h = getFieldType("f.g.h", schema)

    assert(a.get.isInstanceOf[IntegerType])
    assert(b.get.isInstanceOf[StructType])
    assert(c.get.isInstanceOf[IntegerType])
    assert(d.get.isInstanceOf[StructType])
    assert(e.get.isInstanceOf[IntegerType])
    assert(f.get.isInstanceOf[StructType])
    assert(g.get.isInstanceOf[ArrayType])
    assert(h.get.isInstanceOf[IntegerType])
    assert(getFieldType("z", schema).isEmpty)
    assert(getFieldType("x.y.z", schema).isEmpty)
    assert(getFieldType("f.g.h.a", schema).isEmpty)
  }

  test("Testing getFirstArrayPath") {
    assertResult("f.g")(getFirstArrayPath("f.g.h", schema))
    assertResult("f.g")(getFirstArrayPath("f.g", schema))
    assertResult("")(getFirstArrayPath("z.x.y", schema))
    assertResult("")(getFirstArrayPath("b.c.d.e", schema))
  }

  test("Testing getAllArrayPaths") {
    assertResult(Seq("f.g"))(getAllArrayPaths(schema))
    assertResult(Seq())(getAllArrayPaths(schema("b").dataType.asInstanceOf[StructType]))
  }

  test("Testing getAllArraysInPath") {
    assertResult(Seq("b", "b.c.d"))(getAllArraysInPath("b.c.d.e", nestedSchema))
  }

  test("Testing getFieldNameOverriddenByMetadata") {
    assertResult("a")(getFieldNameOverriddenByMetadata(structFieldNoMetadata))
    assertResult("a")(getFieldNameOverriddenByMetadata(structFieldWithMetadataNotSourceColumn))
    assertResult("override_a")(getFieldNameOverriddenByMetadata(structFieldWithMetadataSourceColumn))
  }

  test("Testing getFieldNullability") {
    assert(!getFieldNullability("a", schema).get)
    assert(getFieldNullability("b.d", schema).get)
    assert(getFieldNullability("x.y.z", schema).isEmpty)
  }

  test ("Test isCastAlwaysSucceeds()") {
    assert(!isCastAlwaysSucceeds(StructType(Seq()), StringType))
    assert(!isCastAlwaysSucceeds(ArrayType(StringType), StringType))
    assert(!isCastAlwaysSucceeds(StringType, ByteType))
    assert(!isCastAlwaysSucceeds(StringType, ShortType))
    assert(!isCastAlwaysSucceeds(StringType, IntegerType))
    assert(!isCastAlwaysSucceeds(StringType, LongType))
    assert(!isCastAlwaysSucceeds(StringType, DecimalType(10,10)))
    assert(!isCastAlwaysSucceeds(StringType, DateType))
    assert(!isCastAlwaysSucceeds(StringType, TimestampType))
    assert(!isCastAlwaysSucceeds(StructType(Seq()), StructType(Seq())))
    assert(!isCastAlwaysSucceeds(ArrayType(StringType), ArrayType(StringType)))

    assert(!isCastAlwaysSucceeds(ShortType, ByteType))
    assert(!isCastAlwaysSucceeds(IntegerType, ByteType))
    assert(!isCastAlwaysSucceeds(IntegerType, ShortType))
    assert(!isCastAlwaysSucceeds(LongType, ByteType))
    assert(!isCastAlwaysSucceeds(LongType, ShortType))
    assert(!isCastAlwaysSucceeds(LongType, IntegerType))

    assert(isCastAlwaysSucceeds(StringType, StringType))
    assert(isCastAlwaysSucceeds(ByteType, StringType))
    assert(isCastAlwaysSucceeds(ShortType, StringType))
    assert(isCastAlwaysSucceeds(IntegerType, StringType))
    assert(isCastAlwaysSucceeds(LongType, StringType))
    assert(isCastAlwaysSucceeds(DecimalType(10,10), StringType))
    assert(isCastAlwaysSucceeds(DateType, StringType))
    assert(isCastAlwaysSucceeds(TimestampType, StringType))
    assert(isCastAlwaysSucceeds(StringType, StringType))

    assert(isCastAlwaysSucceeds(ByteType, ByteType))
    assert(isCastAlwaysSucceeds(ByteType, ShortType))
    assert(isCastAlwaysSucceeds(ByteType, IntegerType))
    assert(isCastAlwaysSucceeds(ByteType, LongType))
    assert(isCastAlwaysSucceeds(ShortType, ShortType))
    assert(isCastAlwaysSucceeds(ShortType, IntegerType))
    assert(isCastAlwaysSucceeds(ShortType, LongType))
    assert(isCastAlwaysSucceeds(IntegerType, IntegerType))
    assert(isCastAlwaysSucceeds(IntegerType, LongType))
    assert(isCastAlwaysSucceeds(LongType, LongType))
    assert(isCastAlwaysSucceeds(DateType, TimestampType))
  }

  test("Test isCommonSubPath()") {
    assert (isCommonSubPath())
    assert (isCommonSubPath("a"))
    assert (isCommonSubPath("a.b.c.d.e.f", "a.b.c.d", "a.b.c", "a.b", "a"))
    assert (!isCommonSubPath("a.b.c.d.e.f", "a.b.c.x", "a.b.c", "a.b", "a"))
  }

  test("Test getDeepestCommonArrayPath() for a path without an array") {
    val schema = StructType(Seq[StructField](
      StructField("a",
        StructType(Seq[StructField](
          StructField("b", StringType))
        ))))

    assert (getDeepestCommonArrayPath(schema, Seq("a", "a.b")).isEmpty)
  }

  test("Test getDeepestCommonArrayPath() for a path with a single array at top level") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
          StructField("b", StringType)))
        ))))

    val deepestPath = getDeepestCommonArrayPath(schema, Seq("a", "a.b"))

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a")
  }

  test("Test getDeepestCommonArrayPath() for a path with a single array at nested level") {
    val schema = StructType(Seq[StructField](
      StructField("a", StructType(Seq[StructField](
          StructField("b", ArrayType(StringType))))
        )))

    val deepestPath = getDeepestCommonArrayPath(schema, Seq("a", "a.b"))

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b")
  }

  test("Test getDeepestCommonArrayPath() for a path with several nested arrays of struct") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StructType(Seq[StructField](
          StructField("c", ArrayType(StructType(Seq[StructField](
            StructField("d", StructType(Seq[StructField](
              StructField("e", StringType))
            )))
          ))))
        )))
      )))))

    val deepestPath = getDeepestCommonArrayPath(schema, Seq("a", "a.b", "a.b.c.d.e", "a.b.c.d"))

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b.c")
  }

  test("Test getClosestUniqueName() is working properly") {
    val schema = StructType(Seq[StructField](
      StructField("value", StringType)))

    // A column name that does not exist
    val name1 = SchemaUtils.getClosestUniqueName("v", schema)
    // A column that exists
    val name2 = SchemaUtils.getClosestUniqueName("value", schema)

    assert(name1 == "v")
    assert(name2 == "value_1")
  }

  test("Test isOnlyField()") {
    val schema = StructType(Seq[StructField](
      StructField("a", StringType),
      StructField("b", StructType(Seq[StructField](
        StructField("e", StringType),
        StructField("f", StringType)
      ))),
      StructField("c", StructType(Seq[StructField](
        StructField("d", StringType)
      )))
    ))

    assert(!isOnlyField(schema, "a"))
    assert(!isOnlyField(schema, "a"))
    assert(!isOnlyField(schema, "a"))
    assert(!isOnlyField(schema, "b.e"))
    assert(!isOnlyField(schema, "b.f"))
    assert(isOnlyField(schema, "c.d"))
  }

}
