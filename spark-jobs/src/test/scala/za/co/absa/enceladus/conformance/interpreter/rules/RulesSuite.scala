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

package za.co.absa.enceladus.conformance.interpreter.rules

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.interpreter.rules.mapping.{CommonMappingRuleInterpreter, MappingRuleInterpreterGroupExplode}
import za.co.absa.enceladus.conformance.interpreter.{ExplosionState, InterpreterContextArgs}
import za.co.absa.enceladus.conformance.samples.EmployeeConformance
import za.co.absa.enceladus.dao.EnceladusDAO
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase


class RulesSuite extends AnyFunSuite with TZNormalizedSparkTestBase {

  private val dummyInterpreter = new RuleInterpreter {
    override def conformanceRule: Option[ConformanceRule] = None
    def conform(df: Dataset[Row])
               (implicit spark: SparkSession, explosionState: ExplosionState, dao: EnceladusDAO, progArgs: InterpreterContextArgs): Dataset[Row] = df
  }

  test("Test country code join condition") {
    val countryRule = EmployeeConformance.countryRule
    val countryCondGen = CommonMappingRuleInterpreter.getJoinCondition(countryRule).expr
    val countryCond = (col(s"${CommonMappingRuleInterpreter.inputDfAlias}.country") === col(s"${CommonMappingRuleInterpreter.mappingTableAlias}.country_code")).expr

    assert(countryCondGen.semanticEquals(countryCond))
  }

  test("Test department join condition") {
    val deptRule = EmployeeConformance.departmentRule
    val deptCondGen = CommonMappingRuleInterpreter.getJoinCondition(deptRule).expr
    val deptCond = (col(s"${CommonMappingRuleInterpreter.inputDfAlias}.dept") === col(s"${CommonMappingRuleInterpreter.mappingTableAlias}.dept_id")).expr

    assert(deptCondGen.semanticEquals(deptCond))
  }

  test("Test role join condition") {
    val roleRule = EmployeeConformance.roleRule
    val roleCondGen = CommonMappingRuleInterpreter.getJoinCondition(roleRule).expr
    val roleCond = (
        (col(s"${CommonMappingRuleInterpreter.inputDfAlias}.role") <=> col(s"${CommonMappingRuleInterpreter.mappingTableAlias}.role_id")) &&
        (col(s"${CommonMappingRuleInterpreter.inputDfAlias}.conformed_country") <=> col(s"${CommonMappingRuleInterpreter.mappingTableAlias}.country"))
      ).expr

    assert(roleCondGen.semanticEquals(roleCond))
  }

  test("Test empty join condition evaluates to true") {
    val countryRule = EmployeeConformance.countryRule.copy(attributeMappings = Map.empty)
    val countryCondGen = CommonMappingRuleInterpreter.getJoinCondition(countryRule).expr
    val countryCond = lit(true).expr

    assert(countryCondGen.semanticEquals(countryCond))
  }

  test("Infest strictest type int") {
    val colGen = dummyInterpreter.inferStrictestType("2").expr
    val colMan = lit(2).expr

    assert(colGen.semanticEquals(colMan))
  }

  test("Infest strictest type long") {
    val colGen = dummyInterpreter.inferStrictestType((Long.MaxValue - 1).toString).expr
    val colMan = lit(Long.MaxValue - 1).expr

    assert(colGen.semanticEquals(colMan))
  }

  test("Infest strictest type double") {
    val colGen = dummyInterpreter.inferStrictestType("1234.567").expr
    val colMan = lit(1234.567d).expr

    assert(colGen.semanticEquals(colMan))
  }

  test("Infest strictest type bool") {
    val colGen = dummyInterpreter.inferStrictestType("true").expr
    val colMan = lit(true).expr

    assert(colGen.semanticEquals(colMan))
  }

  test("Infest strictest type string") {
    val colGen = dummyInterpreter.inferStrictestType("MySourceSystem").expr
    val colMan = lit("MySourceSystem").expr

    assert(colGen.semanticEquals(colMan))
  }

  test("Default value of mapping table validation test") {
    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("price", DecimalType(10,6)),
        StructField("orders", StructType(Array(
          StructField("orderdate", DateType),
          StructField("delivertime", TimestampType),
          StructField("happy", BooleanType),
          StructField("system", StructType(Array(
            StructField("name", StringType),
            StructField("description", StringType)))
          )))
        )
      ))

    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("id_test", schema, "id", "1")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("name_test", schema, "name", "'test'")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("name_null_ok", schema, "name", "null")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("decimal_test", schema, "price", "1.6127")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("date_test", schema, "orders.orderdate", "'2017-10-25'")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("timestamp_test", schema, "orders.delivertime", "'2017-10-25 08:35:43'")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("boolean_test", schema, "orders.happy", "true")
    CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("struct_test", schema, "orders.system", "struct('Unknown' as name, 'None' as description)")

    assert(intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("no_attribute_test", schema, "code", "")
    }.getMessage contains "does not contain the specified target attribute")

    assert(intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("not_string", schema, "name", "struct('Unknown' as name)")
    }.getMessage contains "A string expected")

    intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("id_null", schema, "id", "null")
    }

    intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("id_test", schema, "id", "wrong")
    }

    assert(intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("decimal_test", schema, "price", "12345.67")
    }.getMessage contains "Scale/precision don't match the value")

    assert(intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("date_test", schema, "orders.orderdate", "'25/10/2017'")
    }.getMessage contains "Make sure the value matches 'yyyy-MM-dd'")

    assert(intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("timestamp_test", schema, "orders.delivertime", "'25-10-201708:25:43'")
    }.getMessage contains "Make sure the value matches 'yyyy-MM-dd HH:mm:ss'")

    intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("boolean_test", schema, "orders.happy", "a")
    }
    intercept[ValidationException] {
      CommonMappingRuleInterpreter.ensureDefaultValueMatchSchema("struct_test", schema, "orders.system", "struct('unknown' as name)")
    }

  }
}
