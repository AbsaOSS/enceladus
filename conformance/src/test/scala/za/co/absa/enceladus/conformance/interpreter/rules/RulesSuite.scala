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
import org.scalatest.FunSuite
import za.co.absa.enceladus.conformance.CmdConfig
import za.co.absa.enceladus.conformance.interpreter.ExplosionState
import za.co.absa.enceladus.dao.MenasDAO
import za.co.absa.enceladus.model.conformanceRule.ConformanceRule
import za.co.absa.enceladus.samples.EmployeeConformance
import za.co.absa.enceladus.utils.testUtils.SparkTestBase


class RulesSuite extends FunSuite with SparkTestBase {

  private val dummyInterpreter = new RuleInterpreter {
    override def conformanceRule: Option[ConformanceRule] = None
    def conform(df: Dataset[Row])
               (implicit spark: SparkSession, explosionState: ExplosionState, dao: MenasDAO, progArgs: CmdConfig): Dataset[Row] = df
  }

  test("Test country code join condition") {
    val countryRule = EmployeeConformance.countryRule
    val countryCondGen = MappingRuleInterpreterGroupExplode.getJoinCondition(countryRule).expr
    val countryCond = (lit(true)
      && (col(s"${MappingRuleInterpreterGroupExplode.inputDfAlias}.country") === col(s"${MappingRuleInterpreterGroupExplode.mappingTableAlias}.country_code"))).expr

    assert(countryCondGen.semanticEquals(countryCond))
  }

  test("Test department join condition") {
    val deptRule = EmployeeConformance.departmentRule
    val deptCondGen = MappingRuleInterpreterGroupExplode.getJoinCondition(deptRule).expr
    val deptCond = (lit(true) &&
      (col(s"${MappingRuleInterpreterGroupExplode.inputDfAlias}.dept") === col(s"${MappingRuleInterpreterGroupExplode.mappingTableAlias}.dept_id"))).expr

    assert(deptCondGen.semanticEquals(deptCond))
  }

  test("Test role join condition") {
    val roleRule = EmployeeConformance.roleRule
    val roleCondGen = MappingRuleInterpreterGroupExplode.getJoinCondition(roleRule).expr
    val roleCond = (lit(true) &&
      (col(s"${MappingRuleInterpreterGroupExplode.inputDfAlias}.role") <=> col(s"${MappingRuleInterpreterGroupExplode.mappingTableAlias}.role_id")) &&
      (col(s"${MappingRuleInterpreterGroupExplode.inputDfAlias}.conformed_country") <=> col(s"${MappingRuleInterpreterGroupExplode.mappingTableAlias}.country"))).expr

    assert(roleCondGen.semanticEquals(roleCond))
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

    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("id_test", schema, "id", "1")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("name_test", schema, "name", "'test'")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("name_null_ok", schema, "name", "null")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("decimal_test", schema, "price", "1.6127")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("date_test", schema, "orders.orderdate", "'2017-10-25'")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("timestamp_test", schema, "orders.delivertime", "'2017-10-25 08:35:43'")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("boolean_test", schema, "orders.happy", "true")
    MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("struct_test", schema, "orders.system", "struct('Unknown' as name, 'None' as description)")

    assert(intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("no_attribute_test", schema, "code", "")
    }.getMessage contains "does not contain the specified target attribute")

    assert(intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("not_string", schema, "name", "struct('Unknown' as name)")
    }.getMessage contains "A string expected")

    intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("id_null", schema, "id", "null")
    }

    intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("id_test", schema, "id", "wrong")
    }

    assert(intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("decimal_test", schema, "price", "12345.67")
    }.getMessage contains "Scale/precision don't match the value")

    assert(intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("date_test", schema, "orders.orderdate", "'25/10/2017'")
    }.getMessage contains "Make sure the value matches 'yyyy-MM-dd'")

    assert(intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("timestamp_test", schema, "orders.delivertime", "'25-10-201708:25:43'")
    }.getMessage contains "Make sure the value matches 'yyyy-MM-dd HH:mm:ss'")

    intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("boolean_test", schema, "orders.happy", "a")
    }
    intercept[ValidationException] {
      MappingRuleInterpreterGroupExplode.ensureDefaultValueMatchSchema("struct_test", schema, "orders.system", "struct('unknown' as name)")
    }

  }
}
