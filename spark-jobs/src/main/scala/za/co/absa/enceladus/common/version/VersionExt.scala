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

import za.co.absa.commons.version.{NumericComponent, Version}

object VersionExt {

  implicit class RichVersion(version: Version) {
    /**
     * This strips the version to what the final version should look like - only keeping the 3 first numeric components,
     * e.g. `1.2.3-rc.2+567` -> `1.2.3`
     *
     * @return version object with 3 leading numeric values.
     */
    def finalVersion: Version = {
      val leadingComponents = version.components.take(3)
      require(leadingComponents.forall(_.isInstanceOf[NumericComponent]),
        s"The $version is not a valid SemVer - expected 3 numeric components first to derive final version, but got: $leadingComponents")
      // TODO simplify this using version.truncateLabels as suggested in issue "commons#13"

      Version(leadingComponents: _*)
    }
  }

}
