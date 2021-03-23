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

package za.co.absa.enceladus.model.dataFrameFilter

import org.apache.spark.sql.functions.{col, lit, not => columnNot}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class DataFrameFilterSuite extends AnyFunSuite {
  test("Filter for equal value") {
    val filterExpr = EqualsFilter("column1", "v").filter.expr
    val expected = (col("column1") === lit("v")).expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Filter for different value") {
    val filterExpr = DiffersFilter("column2", "2020-11-30", DateType).filter.expr
    val expected = (col("column2") =!= lit("2020-11-30").cast(DateType)).expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Filter if column is Null") {
    val filterExpr = IsNullFilter("column3").filter.expr
    val expected = col("column3").isNull.expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Filter if column is NOT Null") {
    val expected = columnNot(col("column4").isNull).expr
    val filterExpr1 = (!IsNullFilter("column4")).filter.expr
    assert(filterExpr1.semanticEquals(expected))
    val filterExpr2 = not(IsNullFilter("column4")).filter.expr
    assert(filterExpr2.semanticEquals(expected))
  }

  test("Two filters joined with an or condition") {
    val f1 = EqualsFilter("column1", "v1")
    val f2 = EqualsFilter("column1", "v2", StringType)
    val expected = ((col("column1") === "v1") or (col("column1") === lit("v2"))).expr
    val filterExpr1 = OrJoinedFilters(Set(f1, f2)).filter.expr
    assert(filterExpr1.semanticEquals(expected))
    val filterExpr2 = (f1 or f2).filter.expr
    assert(filterExpr2.semanticEquals(expected))
  }

  test("Three filters joined with an and condidion") {
    val f1 = DiffersFilter("column1", "v1")
    val f2 = DiffersFilter("column2", "v2")
    val f3 = DiffersFilter("column3", "v3")
    val expected = ((col("column1") =!= lit("v1")) and (col("column2") =!= lit("v2")) and (col("column3") =!= lit("v3"))).expr
    val filterExpr1 = AndJoinedFilters(Set(f1, f2, f3)).filter.expr
    assert(filterExpr1.semanticEquals(expected))
    val filterExpr2 = (f1 and f2 and f3).filter.expr
    assert(filterExpr2.semanticEquals(expected))
  }

  test("Filter with or within and") {
    val f1 = EqualsFilter("column1", "v1")
    val f2 = EqualsFilter("column2", "2", IntegerType)
    val f3 = EqualsFilter("column2", "3", IntegerType)
    val expected = ((col("column1") === lit("v1")) and
      ((col("column2") === lit("2").cast(IntegerType)) or (col("column2") === lit("3").cast(IntegerType)))).expr
    val filterExpr = AndJoinedFilters(Set(f1, OrJoinedFilters(Set(f2, f3)))).filter.expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Filter with and within or") {
    val f1 = DiffersFilter("column1", "v1")
    val f2 = EqualsFilter("column2", "2", FloatType)
    val f3 = EqualsFilter("column2", "3.14", FloatType)
    val expected = (((col("column1") =!= "v1") and
      (col("column2") === lit("2").cast(FloatType))) or (col("column2") === lit("3.14").cast(FloatType))).expr
    val filterExpr = OrJoinedFilters(Set(AndJoinedFilters(Set(f1, f2)), f3)).filter.expr
    assert(filterExpr.semanticEquals(expected))
  }
}
