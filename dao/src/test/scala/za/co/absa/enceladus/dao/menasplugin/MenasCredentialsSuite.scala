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

package za.co.absa.enceladus.dao.menasplugin

import org.scalatest.FunSuite
import za.co.absa.enceladus.utils.testUtils.SparkTestBase

class MenasCredentialsSuite extends FunSuite with SparkTestBase {

  test("MenasCredentials should be read from *.conf") {
    val credentials = MenasCredentials.fromFile("src/test/resources/menas-credentials.conf")
    assert(credentials == MenasCredentials("user", "changeme"))
  }

  test("MenasCredentials should be read from *.properties") {
    val credentials = MenasCredentials.fromFile("src/test/resources/menas-credentials.properties")
    assert(credentials == MenasCredentials("user", "changeme"))
  }

  test("MenasCredentials should be read from *.yml") {
    val credentials = MenasCredentials.fromFile("src/test/resources/menas-credentials.yml")
    assert(credentials == MenasCredentials("user", "changeme"))
  }

  test("MenasCredentials should replace tilde ('~') with home dir") {
    val homeDir = System.getProperty("user.home")
    val expected = s"$homeDir/dir/file"

    val actual = MenasCredentials.replaceHome("~/dir/file")
    assert(actual == expected)
  }
}
