package za.co.absa.enceladus.conformance.config

import org.scalatest.FunSuite
import za.co.absa.enceladus.model.dataFrameFilter._

class FilterFromConfigSuite extends FunSuite {
  test("Filter for dataset doesn't exist") {
    //assert(FilterFromConfig.loadFilter("NotExistent").isEmpty)
  }

  test("Filter for dataset is empty") {
    assert(FilterFromConfig.loadFilter("Empty").isEmpty)
  }

  test("Filter for dataset is set") {
    val expected = Some(EqualFilter("myColumn", "This value"))
    assert(FilterFromConfig.loadFilter("OK") == expected)
  }

  test("Filter for dataset is wrong") {
    assert(FilterFromConfig.loadFilter("Fail").isEmpty)
  }

  test("A complex filter") {
    val f1 = EqualFilter("myColumn", "This value")
    val f2 = DifferFilter("myColumn2", "Any other value")
    val expected = Some(AndJoinedFilters(Set(f1, f2)))
    assert(FilterFromConfig.loadFilter("Complex") == expected)
  }
}
