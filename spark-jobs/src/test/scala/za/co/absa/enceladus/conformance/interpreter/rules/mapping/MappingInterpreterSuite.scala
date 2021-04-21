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

package za.co.absa.enceladus.conformance.interpreter.rules.mapping

import org.apache.commons.io.IOUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import za.co.absa.enceladus.conformance.interpreter.rules.testcasefactories.{NestedTestCaseFactory, SimpleTestCaseFactory}
import za.co.absa.enceladus.utils.testUtils.{HadoopFsTestBase, LoggerTestBase, SparkTestBase}

trait MappingInterpreterSuite extends AnyFunSuite with SparkTestBase with LoggerTestBase with BeforeAndAfterAll with HadoopFsTestBase{

  protected val simpleTestCaseFactory = new SimpleTestCaseFactory()
  protected val nestedTestCaseFactory = new NestedTestCaseFactory()

  override def beforeAll(): Unit = {
    super.beforeAll()
    simpleTestCaseFactory.createMappingTables()
    nestedTestCaseFactory.createMappingTables()
  }

  override def afterAll(): Unit = {
    simpleTestCaseFactory.deleteMappingTables()
    nestedTestCaseFactory.deleteMappingTables()
    super.afterAll()
  }

  // todo: connected to #1756 for array-based processing, errCol nullability always comes out false after Spark 3.1 processing
  protected def normalizeErrColNullability(inputSchemaTree: String): String = {
    inputSchemaTree
      .replaceAll("errCol: array \\(nullable = false\\)", "errCol: array (nullable = true)")
      .trim
  }

  protected def cleanupContainsNullProperty(inputSchemaTree: String): String = {
    // This cleanup is needed since when a struct is processed via nestedStructMap() or nestedStructAndErrorMap(),
    // the new version of the struct always has the flag containsNull = false.
    inputSchemaTree
      .replaceAll("\\ \\(containsNull = true\\)", "")
      .replaceAll("\\ \\(containsNull = false\\)", "")
      .trim
  }

  protected def getResourceString(name: String): String =
    IOUtils.toString(getClass.getResourceAsStream(name), "UTF-8")

  protected def assertSchema(actualSchema: String, expectedSchema: String): Unit = {
    if ( fixLineEnding(actualSchema.trim) != fixLineEnding(expectedSchema.trim)) {
      logger.error("EXPECTED:")
      logger.error(expectedSchema)
      logger.error("ACTUAL:")
      logger.error(actualSchema)
      fail("Actual conformed schema does not match the expected schema (see above).")
    }
  }

  protected def assertResults(actualResults: String, expectedResults: String): Unit = {
    if (!fixLineEnding(expectedResults).startsWith(fixLineEnding(actualResults))) {
      logger.error("EXPECTED:")
      logger.error(expectedResults)
      logger.error("ACTUAL:")
      logger.error(actualResults)
      fail("Actual conformed dataset JSON does not match the expected JSON (see above).")
    }
  }

  /**
   * When the project is git cloned on Windows all text files might end up having CR LF line ending.
   * (This depends on git settings)
   * In order to make the tests line ending agnostic we need to replace CR LF with Unix line endings (LF).
   *
   * @param s A multiline string.
   * @return The string with line endings fixed.
   * */
  protected def fixLineEnding(s: String): String = s.replace("\r\n", "\n")

}
