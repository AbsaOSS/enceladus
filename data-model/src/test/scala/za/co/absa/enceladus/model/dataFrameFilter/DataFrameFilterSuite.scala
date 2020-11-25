package za.co.absa.enceladus.model.dataFrameFilter

import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class DataFrameFilterSuite extends FunSuite {
  test("Filter for equal value") {
    val filterExpr = EqualFilter("column1", "v").filter.expr
    val expected = (col("column1") === "v").expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Filter for different value") {
    val filterExpr = DifferFilter("column2", "diff").filter.expr
    val expected = (col("column2") =!= "diff").expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Two filters joined with an or condition") {
    val f1 = EqualFilter("column1", "v1")
    val f2 = EqualFilter("column1", "v2")
    val expected = ((col("column1") === "v1") or (col("column1") === "v2")).expr
    val filterExpr1 = OrJoinedFilters(Set(f1, f2)).filter.expr
    assert(filterExpr1.semanticEquals(expected))
    val filterExpr2 = (f1 or f2).filter.expr
    assert(filterExpr2.semanticEquals(expected))
  }

  test("Three filters joined with an and condidion") {
    val f1 = DifferFilter("column1", "v1")
    val f2 = DifferFilter("column2", "v2")
    val f3 = DifferFilter("column3", "v3")
    val expected = ((col("column1") =!= "v1") and (col("column2") =!= "v2") and (col("column3") =!= "v3")).expr
    val filterExpr1 = AndJoinedFilters(Set(f1, f2, f3)).filter.expr
    assert(filterExpr1.semanticEquals(expected))
    val filterExpr2 = (f1 and f2 and f3).filter.expr
    assert(filterExpr2.semanticEquals(expected))
  }

  test("Filter with or within and") {
    val f1 = EqualFilter("column1", "v1")
    val f2 = EqualFilter("column2", "v2")
    val f3 = EqualFilter("column2", "v3")
    val expected = ((col("column1") === "v1") and ((col("column2") === "v2") or (col("column2") === "v3"))).expr
    val filterExpr = AndJoinedFilters(Set(f1, OrJoinedFilters(Set(f2, f3)))).filter.expr
    assert(filterExpr.semanticEquals(expected))
  }

  test("Filter with and within or") {
    val f1 = DifferFilter("column1", "v1")
    val f2 = DifferFilter("column2", "v2")
    val f3 = DifferFilter("column3", "v3")
    val expected = (((col("column1") =!= "v1") and (col("column2") =!= "v2")) or (col("column3") =!= "v3")).expr
    val filterExpr = OrJoinedFilters(Set(AndJoinedFilters(Set(f1, f2)), f3)).filter.expr
    assert(filterExpr.semanticEquals(expected))
  }
}
