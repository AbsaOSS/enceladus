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

package za.co.absa.enceladus.conformance.config

import java.text.ParseException

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.model.dataFrameFilter._

class FilterFromConfigSuite extends AnyFunSuite {

  test("Filter for dataset doesn't exist") {
    assert(FilterFromConfig.loadFilter("NotExistent").isEmpty)
  }

  test("Filter for dataset is empty") {
    assert(FilterFromConfig.loadFilter("Empty").isEmpty)
  }

  test("Filter for dataset is set") {
    val valueType: String = null
    val expected = EqualsFilter("myColumn", "This value", valueType)
    val loaded = FilterFromConfig.loadFilter("OK").get
    assert(loaded == expected)
    assert(loaded.asInstanceOf[EqualsFilter].dataType == StringType)
  }

  test("Filter for dataset is wrong") {
    val filterName = "Fail"

    val except = intercept[ParseException] {
      FilterFromConfig.loadFilter(filterName)
    }
    assert(except.getMessage.contains(s"$filterName filter load failed"))
  }

  test("A complex filter") {
    val valueType: String = null
    val f1 = EqualsFilter("myColumn", "This value", valueType)
    val f2 = DiffersFilter("myColumn2", "2", IntegerType)
    val expected = AndJoinedFilters(Set(f1, f2))
    val loaded = FilterFromConfig.loadFilter("Complex").get
    assert(loaded == expected)
    val types = loaded.asInstanceOf[AndJoinedFilters].filterItems.map(item => item.asInstanceOf[SingleColumnAndValueFilter].dataType)
    assert(types == Set(StringType, IntegerType))
  }
}
