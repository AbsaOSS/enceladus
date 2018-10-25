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

import org.scalatest.FunSuite
import za.co.absa.enceladus.samples.EmployeeConformance
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class DatasourceSuite extends FunSuite with SparkTestBase {

  test("Data Source loads all data needed for test sample") {

    val inputDf = DataSource.getData(EmployeeConformance.employeeDS.hdfsPath, "2017", "11", "01", "{0}/{1}/{2}")

    assert(inputDf.columns.toList.sorted === List("employee_id", "name", "surname", "dept", "role", "country").sorted)

  }

}
