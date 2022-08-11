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

package za.co.absa.enceladus.dao.rest.auth

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.enceladus.dao.auth.RestApiPlainCredentials
import za.co.absa.enceladus.utils.fs.LocalFsUtils
import za.co.absa.enceladus.utils.testUtils.TZNormalizedSparkTestBase

class RestApiPlainCredentialsSuite extends AnyWordSpec with TZNormalizedSparkTestBase {

  "RestApiPlainCredentials" should {
    "be read from *.conf" in {
      val credentials = RestApiPlainCredentials.fromFile("src/test/resources/rest-api-credentials.conf")
      assert(credentials == RestApiPlainCredentials("user", "changeme"))
    }

    "be read from *.properties" in {
      val credentials = RestApiPlainCredentials.fromFile("src/test/resources/rest-api-credentials.properties")
      assert(credentials == RestApiPlainCredentials("user", "changeme"))
    }

    "be read from *.yml" in {
      val credentials = RestApiPlainCredentials.fromFile("src/test/resources/rest-api-credentials.yml")
      assert(credentials == RestApiPlainCredentials("user", "changeme"))
    }

    "replace tilde ('~') with home dir" in {
      val homeDir = System.getProperty("user.home")
      val expected = s"$homeDir/dir/file"

      val actual = LocalFsUtils.replaceHome("~/dir/file")
      assert(actual == expected)
    }
  }

}
