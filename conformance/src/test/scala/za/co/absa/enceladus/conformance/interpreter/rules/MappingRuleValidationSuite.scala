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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.datasource.DataSource
import za.co.absa.enceladus.model.conformanceRule.MappingConformanceRule
import za.co.absa.enceladus.samples.EmployeeConformance
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class MappingRuleValidationSuite extends FunSuite with SparkTestBase {
  // scalastyle:off line.size.limit

  test("Mapping rule fields existence validation test") {

    val df = DataSource.getData(EmployeeConformance.employeeDS.hdfsPath, "2017", "11", "01", "{0}/{1}/{2}")
    val mappingTable = EmployeeConformance.countryMT
    val mapTable = DataSource.getData(mappingTable.hdfsPath, "2017", "11", "01", "reportDate={0}-{1}-{2}")

    val rule1 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country"), targetAttribute = "country_name", outputColumn =
        "conformed_country")
    MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule1)

    val rule2 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country"), targetAttribute = "Country_name", outputColumn = "conformed_country")
    val except2 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule2)
    }
    assert(except2.message.contains("Column name 'Country_name' does not case-sensitively match 'country_name'"))

    val rule3 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_code" -> "Country"), targetAttribute = "country_name", outputColumn = "conformed_country")
    val except3 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule3)
    }
    assert(except3.message.contains("Column name 'Country' does not case-sensitively match 'country'"))

    val rule4 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_Code" -> "country"), targetAttribute = "country_name", outputColumn = "conformed_country")
    val except4 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule4)
    }
    assert(except4.message.contains("Column name 'country_Code' does not case-sensitively match 'country_code'"))

    val rule5 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country"), targetAttribute = "country_name", outputColumn = "country.conformed_country")
    val except5 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule5)
    }
    assert(except5.message.contains("Column 'country' is a primitive type and can't contain child fields 'country.conformed_country'"))

    val rule6 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_zip" -> "country"), targetAttribute = "country_name", outputColumn = "conformed_country")
    val except6 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule6)
    }
    assert(except6.message.contains("Column name 'country_zip' does not exist"))

    val rule7 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country_id"), targetAttribute = "country_name", outputColumn = "conformed_country")
    val except7 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule7)
    }
    assert(except7.message.contains("Column name 'country_id' does not exist"))

    val rule8 = new MappingConformanceRule(order = 0, mappingTable = "country", controlCheckpoint = false,
      mappingTableVersion = 0, attributeMappings = Map("country_code" -> "country"), targetAttribute = "country_description", outputColumn = "conformed_country")
    val except8 = intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.validateMappingFieldsExist("", df.schema, mapTable.schema, rule8)
    }
    assert(except8.message.contains("olumn name 'country_description' does not exist"))
  }


}
