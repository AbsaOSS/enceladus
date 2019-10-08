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

  private val schema = StructType(Seq(
    StructField("a", IntegerType, nullable = false),
    StructField("b", StructType(Seq(
      StructField("c", IntegerType),
      StructField("d", StructType(Seq(
        StructField("e", IntegerType))), nullable = true)))),
    StructField("f", StructType(Seq(
      StructField("g", ArrayType.apply(StructType(Seq(
        StructField("h", IntegerType))))))))))

  private val nestedSchema = StructType(Seq(
    StructField("a", IntegerType),
    StructField("b", ArrayType(StructType(Seq(
      StructField("c", StructType(Seq(
        StructField("d", ArrayType(StructType(Seq(
          StructField("e", IntegerType))))))))))))))

  private val arrayOfArraysSchema = StructType(Seq(
    StructField("a", ArrayType(ArrayType(IntegerType)), nullable = false),
    StructField("b", ArrayType(ArrayType(StructType(Seq(
        StructField("c", StringType, nullable = false)
      ))
    )), nullable = true)
  ))

  private val structFieldNoMetadata = StructField("a", IntegerType)

  private val structFieldWithMetadataNotSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("meta", "data").build)
  private val structFieldWithMetadataSourceColumn = StructField("a", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "override_a").build)

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

  test("Testing fieldExists") {
    assert(fieldExists("a", schema))
    assert(fieldExists("b", schema))
    assert(fieldExists("b.c", schema))
    assert(fieldExists("b.d", schema))
    assert(fieldExists("b.d.e", schema))
    assert(fieldExists("f", schema))
    assert(fieldExists("f.g", schema))
    assert(fieldExists("f.g.h", schema))
    assert(!fieldExists("z", schema))
    assert(!fieldExists("x.y.z", schema))
    assert(!fieldExists("f.g.h.a", schema))
  }

  test ("Test isColumnArrayOfStruct") {
    assert(!isColumnArrayOfStruct("a", schema))
    assert(!isColumnArrayOfStruct("b", schema))
    assert(!isColumnArrayOfStruct("b.c", schema))
    assert(!isColumnArrayOfStruct("b.d", schema))
    assert(!isColumnArrayOfStruct("b.d.e", schema))
    assert(!isColumnArrayOfStruct("f", schema))
    assert(isColumnArrayOfStruct("f.g", schema))
    assert(!isColumnArrayOfStruct("f.g.h", schema))
    assert(!isColumnArrayOfStruct("a", nestedSchema))
    assert(isColumnArrayOfStruct("b", nestedSchema))
    assert(isColumnArrayOfStruct("b.c.d", nestedSchema))
  }

  test("getRenamesInSchema - no renames") {
    val result = getRenamesInSchema(StructType(Seq(
      structFieldNoMetadata,
      structFieldWithMetadataNotSourceColumn)))
    assert(result.isEmpty)
  }

  test("getRenamesInSchema - simple rename") {
    val result = getRenamesInSchema(StructType(Seq(structFieldWithMetadataSourceColumn)))
    assert(result == Map("a" -> "override_a"))

  }

  test("getRenamesInSchema - complex with includeIfPredecessorChanged set") {
    val sub = StructType(Seq(
      StructField("d", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "o").build),
      StructField("e", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "e").build),
      StructField("f", IntegerType)
    ))
    val schema = StructType(Seq(
      StructField("a", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "x").build),
      StructField("b", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "b").build),
      StructField("c", sub)
    ))

    val includeIfPredecessorChanged = true
    val result = getRenamesInSchema(schema, includeIfPredecessorChanged)
    val expected = Map(
      "a"   -> "x"  ,
      "a.d" -> "x.o",
      "a.e" -> "x.e",
      "a.f" -> "x.f",
      "b.d" -> "b.o",
      "c.d" -> "c.o"
    )

    assert(result == expected)
  }

  test("getRenamesInSchema - complex with includeIfPredecessorChanged not set") {
    val sub = StructType(Seq(
      StructField("d", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "o").build),
      StructField("e", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "e").build),
      StructField("f", IntegerType)
    ))
    val schema = StructType(Seq(
      StructField("a", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "x").build),
      StructField("b", sub, nullable = false, new MetadataBuilder().putString("sourcecolumn", "b").build),
      StructField("c", sub)
    ))

    val includeIfPredecessorChanged = false
    val result = getRenamesInSchema(schema, includeIfPredecessorChanged)
    val expected = Map(
      "a"   -> "x",
      "a.d" -> "x.o",
      "b.d" -> "b.o",
      "c.d" -> "c.o"
    )

    assert(result == expected)
  }


  test("getRenamesInSchema - array") {
    val sub = StructType(Seq(
      StructField("renamed", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "rename source").build),
      StructField("same", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "same").build),
      StructField("f", IntegerType)
    ))
    val schema = StructType(Seq(
      StructField("array1", ArrayType(sub)),
      StructField("array2", ArrayType(ArrayType(ArrayType(sub)))),
      StructField("array3", ArrayType(IntegerType), nullable = false, new MetadataBuilder().putString("sourcecolumn", "array source").build)
    ))

    val includeIfPredecessorChanged = false
    val result = getRenamesInSchema(schema, includeIfPredecessorChanged)
    val expected = Map(
      "array1.renamed" -> "array1.rename source",
      "array2.renamed" -> "array2.rename source",
      "array3"   -> "array source"
    )

    assert(result == expected)
  }


  test("getRenamesInSchema - source column used multiple times") {
    val sub = StructType(Seq(
      StructField("x", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "src").build),
      StructField("y", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "src").build)
    ))
    val schema = StructType(Seq(
      StructField("a", sub),
      StructField("b", IntegerType, nullable = false, new MetadataBuilder().putString("sourcecolumn", "src").build)
    ))

    val result = getRenamesInSchema(schema)
    val expected = Map(
      "a.x" -> "a.src",
      "a.y" -> "a.src",
      "b"   -> "src"
    )

    assert(result == expected)
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

  test("Test getDeepestArrayPath() for a path without an array") {
    val schema = StructType(Seq[StructField](
      StructField("a",
        StructType(Seq[StructField](
          StructField("b", StringType))
        ))))

    assert (getDeepestArrayPath(schema, "a.b").isEmpty)
  }

  test("Test getDeepestArrayPath() for a path with a single array at top level") {
    val schema = StructType(Seq[StructField](
      StructField("a", ArrayType(StructType(Seq[StructField](
        StructField("b", StringType)))
      ))))

    val deepestPath = getDeepestArrayPath(schema, "a.b")

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a")
  }

  test("Test getDeepestArrayPath() for a path with a single array at nested level") {
    val schema = StructType(Seq[StructField](
      StructField("a", StructType(Seq[StructField](
        StructField("b", ArrayType(StringType))))
      )))

    val deepestPath = getDeepestArrayPath(schema, "a.b")
    val deepestPath2 = getDeepestArrayPath(schema, "a")

    assert (deepestPath.nonEmpty)
    assert (deepestPath.get == "a.b")
    assert (deepestPath2.isEmpty)
  }

  test("Test getDeepestArrayPath() for a path with several nested arrays of struct") {
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

    val deepestPath = getDeepestArrayPath(schema, "a.b.c.d.e")

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
    assert(!isOnlyField(schema, "b.e"))
    assert(!isOnlyField(schema, "b.f"))
    assert(isOnlyField(schema, "c.d"))
  }

  test("Test getStructField on array of arrays") {
    assert(getField("a", arrayOfArraysSchema).contains(StructField("a",ArrayType(ArrayType(IntegerType)),nullable = false)))
    assert(getField("b", arrayOfArraysSchema).contains(StructField("b",ArrayType(ArrayType(StructType(Seq(StructField("c",StringType,nullable = false))))), nullable = true)))
    assert(getField("b.c", arrayOfArraysSchema).contains(StructField("c",StringType,nullable = false)))
    assert(getField("b.d", arrayOfArraysSchema).isEmpty)
  }

  test("Test fieldExists") {
    assert(fieldExists("a", schema))
    assert(fieldExists("b", schema))
    assert(fieldExists("b.c", schema))
    assert(fieldExists("b.d", schema))
    assert(fieldExists("b.d.e", schema))
    assert(fieldExists("f", schema))
    assert(fieldExists("f.g", schema))
    assert(fieldExists("f.g.h", schema))
    assert(!fieldExists("z", schema))
    assert(!fieldExists("x.y.z", schema))
    assert(!fieldExists("f.g.h.a", schema))

    assert(fieldExists("a", arrayOfArraysSchema))
    assert(fieldExists("b", arrayOfArraysSchema))
    assert(fieldExists("b.c", arrayOfArraysSchema))
    assert(!fieldExists("b.d", arrayOfArraysSchema))
  }

  test("unpath - empty string remains empty") {
    val result = unpath("")
    val expected = ""
    assert(result == expected)
  }

  test("unpath - underscores get doubled") {
    val result = unpath("one_two__three")
    val expected = "one__two____three"
    assert(result == expected)
  }

  test("unpath - dot notation conversion") {
    val result = unpath("grand_parent.parent.first_child")
    val expected = "grand__parent_parent_first__child"
    assert(result == expected)
  }
}
