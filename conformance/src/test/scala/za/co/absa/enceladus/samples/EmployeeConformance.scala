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

package za.co.absa.enceladus.samples

import za.co.absa.enceladus.model.conformanceRule._
import za.co.absa.enceladus.model.{Dataset, DefaultValue, MappingTable}
import za.co.absa.enceladus.utils.error.ErrorMessage
import za.co.absa.enceladus.utils.error.Mapping

object EmployeeConformance {
  val countryMT = new MappingTable(name = "country", version = 0, hdfsPath = "src/test/testData/country", schemaName = "country", schemaVersion = 0)
  val departmentMT = new MappingTable(name = "department", version = 0, hdfsPath = "src/test/testData/department", schemaName = "dept", schemaVersion = 0, defaultMappingValue = List(DefaultValue("department_name","'Unknown dept'")))
  val roleMT = new MappingTable(name = "role", version = 0, hdfsPath = "src/test/testData/role", schemaName = "role", schemaVersion = 0, defaultMappingValue = List())

  val countryRule = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
    mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country" ), targetAttribute = "country_name", outputColumn = "conformed_country")

  val departmentRule = new MappingConformanceRule(order = 1, mappingTable = "department", controlCheckpoint = true,
    mappingTableVersion = 0, attributeMappings = Map("dept_id" -> "dept"), targetAttribute = "department_name", outputColumn = "conformed_department")

  val roleRule = new MappingConformanceRule(order = 2, mappingTable = "role", controlCheckpoint = false,
    mappingTableVersion = 0, attributeMappings = Map("role_id" -> "role", "country" -> "conformed_country"), targetAttribute = "role_name", outputColumn = "conformed_role", isNullSafe = true)

  val litRule = LiteralConformanceRule(order = 3, outputColumn = "MyLiteral", controlCheckpoint = true, value = "abcdef")

  val upperRule = UppercaseConformanceRule(order = 4, inputColumn = "MyLiteral", controlCheckpoint = false, outputColumn = "MyUpperLiteral")

  val lit2Rule = LiteralConformanceRule(order = 5, outputColumn = "ToBeDropped", controlCheckpoint = true, value = "123456")

  val dropRule = DropConformanceRule(order = 6, outputColumn = "ToBeDropped", controlCheckpoint = false)

  val concatRule = ConcatenationConformanceRule(order = 7, outputColumn = "Concatenated", controlCheckpoint = true, Seq("MyLiteral", "MyUpperLiteral"))

  val sparkConfRule = SparkSessionConfConformanceRule(order = 8, outputColumn = "SparkConfAttr", controlCheckpoint = false, sparkConfKey = "co.za.absa.enceladus.confTest")

  val singleColRule = SingleColumnConformanceRule(order = 9, outputColumn = "ConformedRoleId", controlCheckpoint = true, inputColumn = "role", inputColumnAlias = "roleId")

  val castingColRules = CastingConformanceRule(order = 10, outputColumn = "ConformedEmployeeId", controlCheckpoint = false, inputColumn = "employee_id", outputDataType = "string")

  val employeeDS = Dataset(name = "Employee Conformance", version = 1, hdfsPath = "src/test/testData/employee", hdfsPublishPath = "testData/conformedEmployee",
      schemaName = "Employee", schemaVersion = 0,
      conformance = List(countryRule, departmentRule, roleRule, litRule, upperRule, lit2Rule, dropRule, concatRule, sparkConfRule, singleColRule, castingColRules) )

  val conformedEmployees = Seq(
    ConformedEmployee(employee_id = 0, name = "John", surname = "Doe0", dept= 0, role = 1, country = "CZE", conformed_country = "Czech Republic", conformed_department = "Tooling Squad",
      conformed_role = "Big Data Engineer", errCol= List(), MyLiteral = "abcdef", MyUpperLiteral = "ABCDEF", Concatenated = "abcdefABCDEF", SparkConfAttr = "hello :)",
      ConformedRole(1), ConformedEmployeeId = "0"),
    ConformedEmployee(employee_id = 1, name = "John", surname = "Doe1", dept= 2, role = 0, country = "SA", conformed_country = "South Africa", conformed_department = "Enablement Squad",
      conformed_role = "He does everything!", errCol= List(), MyLiteral = "abcdef", MyUpperLiteral = "ABCDEF", Concatenated = "abcdefABCDEF", SparkConfAttr = "hello :)",
      ConformedRole(0), ConformedEmployeeId = "1"),
    ConformedEmployee(employee_id = 2, name = "John", surname = "Doe2", dept= 0, role = 1, country = "CZE", conformed_country = "Czech Republic", conformed_department = "Tooling Squad",
      conformed_role = "Big Data Engineer", errCol= List(), MyLiteral = "abcdef", MyUpperLiteral = "ABCDEF", Concatenated = "abcdefABCDEF", SparkConfAttr = "hello :)",
      ConformedRole(1), ConformedEmployeeId = "2"),
    ConformedEmployee(employee_id = 3, name = "John", surname = "Doe3", dept= 3, role = 2, country = "SWE", conformed_country = null, conformed_department = "Unknown dept",
      conformed_role = "External dev", errCol= List(
          ErrorMessage.confMappingErr("conformed_country", Seq("SWE"), Seq(Mapping("country_code", "country"))),
          ErrorMessage.confMappingErr("conformed_department", Seq("3"), Seq(Mapping("dept_id", "dept"))) 
      ), MyLiteral = "abcdef", MyUpperLiteral = "ABCDEF", Concatenated = "abcdefABCDEF", SparkConfAttr = "hello :)", ConformedRole(2), ConformedEmployeeId = "3"),
    ConformedEmployee(employee_id = 4, name = "John", surname = "Doe4", dept= 1, role = 2, country = "IN", conformed_country = "India", conformed_department = "Ingestion Squad",
      conformed_role = "Ingestion Developer", errCol= List(), MyLiteral = "abcdef", MyUpperLiteral = "ABCDEF", Concatenated = "abcdefABCDEF", SparkConfAttr = "hello :)",
      ConformedRole(2), ConformedEmployeeId = "4"),
    ConformedEmployee(employee_id = 5, name = "John", surname = "Doe5", dept= 2, role = 3, country = "SA", conformed_country = "South Africa", conformed_department = "Enablement Squad",
      conformed_role = "The beast!", errCol= List(), MyLiteral = "abcdef", MyUpperLiteral = "ABCDEF", Concatenated = "abcdefABCDEF", SparkConfAttr = "hello :)",
      ConformedRole(3), ConformedEmployeeId = "5")
  )
}

case class ConformedEmployee(employee_id: Int, name: String, surname: String, dept: Int, role: Int, country: String, conformed_country: String, conformed_department: String,
    conformed_role: String, errCol: List[ErrorMessage], MyLiteral: String, MyUpperLiteral: String, Concatenated: String, SparkConfAttr: String, ConformedRoleId: ConformedRole,
    ConformedEmployeeId: String)
    
case class ConformedRole(roleId: Int)
