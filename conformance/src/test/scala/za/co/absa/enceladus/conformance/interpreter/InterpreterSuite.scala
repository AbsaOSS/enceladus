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

package za.co.absa.enceladus.conformance.interpreter

import org.mockito.Mockito.{mock, when => mockWhen}
import org.scalatest.FunSuite
import za.co.absa.atum.model.ControlMeasure
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.samples.{ConformedEmployee, EmployeeConformance}
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

import scala.io.Source

class InterpreterSuite extends FunSuite with SparkTestBase {

  test("End to end dynamic conformance test") {

    // Enable Conformance Framweork
    import za.co.absa.atum.AtumImplicits._
    spark.enableControlMeasuresTracking("src/test/testData/employee/2017/11/01/_INFO", "src/test/testData/_testOutput/_INFO")

    //configure conf value
    spark.sessionState.conf.setConfString("co.za.absa.enceladus.confTest", "hello :)")

    implicit val dao: EnceladusDAO = mock(classOf[EnceladusDAO])
    implicit val progArgs = CmdConfig(reportDate = "2017-11-01")
    implicit val enableCF = true

    import spark.implicits._
    val mappingTablePattern = "{0}/{1}/{2}"

    val dfs = DataSource.getData(EmployeeConformance.employeeDS.hdfsPath, "2017", "11", "01", mappingTablePattern)

    mockWhen(dao.getDataset("Employee Conformance", 1)) thenReturn EmployeeConformance.employeeDS
    mockWhen(dao.getMappingTable("country", 0)) thenReturn EmployeeConformance.countryMT
    mockWhen(dao.getMappingTable("department", 0)) thenReturn EmployeeConformance.departmentMT
    mockWhen(dao.getMappingTable("role", 0)) thenReturn EmployeeConformance.roleMT
    mockWhen(dao.getSchema("Employee", 0)) thenReturn dfs.schema

    val conformed = DynamicInterpreter.interpret(EmployeeConformance.employeeDS, dfs).cache
    val data = conformed.as[ConformedEmployee].collect.sortBy(_.employee_id).toList
    val expected = EmployeeConformance.conformedEmployees.sortBy(_.employee_id).toList

    println("DEBUG: Expected:")
    expected.foreach(println)
    // println(ControlUtils.asJsonPretty(expected))
    println("")

    println("DEBUG: Actual:")
    data.foreach(println)
    // println(ControlUtils.asJsonPretty(data))
    println("")

    assertResult(expected)(data)
    // test drop
    assert(!conformed.columns.contains("ToBeDropped"))

    // perform the write
    conformed.coalesce(1).orderBy($"employee_id" asc).write.mode("overwrite").parquet("src/test/testData/_testOutput")

    val infoFile = Source.fromFile("src/test/testData/_testOutput/_INFO").getLines().mkString("\n")

    import org.json4s._
    import org.json4s.native.JsonMethods._

    implicit val formats: DefaultFormats.type = DefaultFormats

    val checkpoints = parse(infoFile).extract[ControlMeasure].checkpoints

    // check that all the expected checkpoints are there
    assert(checkpoints.lengthCompare(9) == 0)

    checkpoints.foreach({ cp =>
      assert(cp.controls(0).controlValue === 8)
      assert(cp.controls(1).controlValue === 6)
    })

  }

}
