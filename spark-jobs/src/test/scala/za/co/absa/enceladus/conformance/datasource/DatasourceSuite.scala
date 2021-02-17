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

package za.co.absa.enceladus.conformance.datasource

import org.apache.spark.sql.types.IntegerType
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.samples.EmployeeConformance
import za.co.absa.enceladus.model.dataFrameFilter._
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class DatasourceSuite extends AnyFunSuite with SparkTestBase {

  test("Data Source loads all data needed for test sample") {

    val inputDf = DataSource.getDataFrame(EmployeeConformance.employeeDS.hdfsPath, "2017-11-01", "{0}/{1}/{2}")

    assert(inputDf.columns.toList.sorted === List("employee_id", "name", "surname", "dept", "role", "country").sorted)

  }

  test("Data Source loads data simple filtered") {
    val filter = Option(DiffersFilter("country", "CZE"))
    val ds = DataSource.getDataFrame(EmployeeConformance.employeeDS.hdfsPath,"2017-11-01", "{0}/{1}/{2}", filter)
    val resultIds = ds.select("employee_id").collect.toList.map(_.get(0).toString.toInt)
    val expected = List(1, 3, 4, 5)
    assert(resultIds == expected)
  }

  test("Data Source loads data filtered using complex filter") {
    val filter = Option(OrJoinedFilters(Set(EqualsFilter("country", "CZE"), EqualsFilter("role", "2", IntegerType))))
    val ds = DataSource.getDataFrame(EmployeeConformance.employeeDS.hdfsPath,"2017-11-01", "{0}/{1}/{2}", filter)
    val resultIds = ds.select("employee_id").collect.toList.map(_.get(0).toString.toInt)
    val expected = List(0, 2, 3, 4)
    assert(resultIds == expected)
  }
}
