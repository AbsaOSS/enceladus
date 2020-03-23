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

package za.co.absa.enceladus.common.version

import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.commons.version.Version
import za.co.absa.commons.version.Version._

class VersionExtSuite extends FlatSpec with Matchers {

  import VersionExt._

  val expectedFinalVersion = Version.asSemVer("1.2.3")

  "VersionExt" should "correctly derive final version for SemVers" in {
    val toBeFinalized = Seq(
      "1.2.3-alpha", "1.2.3-alpha.1", "1.2.3-0.3.7", "1.2.3-x.7.z.92",
      "1.2.3-dev", "1.2.3-rc.1", "1.2.3-alpha.beta", "1.2.3-alpha.beta+222", "1.2.3"
    )

    toBeFinalized.foreach { toFinalize =>
      Version.asSemVer(toFinalize).finalVersion shouldBe expectedFinalVersion
    }
  }

  it should "only work with compatible simple versions (having first 3 numeric values)" in {
    ver"1.2.3".finalVersion shouldBe expectedFinalVersion
    ver"1.2.3.4.5".finalVersion shouldBe expectedFinalVersion
    ver"1.2.3.foo.bar".finalVersion shouldBe expectedFinalVersion

    val caughtException = the[IllegalArgumentException] thrownBy {
      ver"1.2.3-dev".finalVersion // 3rd component is "3-dev" for simple Version
    }
    caughtException.getMessage should include("is not a valid SemVer - expected 3 numeric components first to derive final version")
  }

}
