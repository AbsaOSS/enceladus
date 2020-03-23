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

import za.co.absa.commons.version.Version
import za.co.absa.enceladus.common.SparkCompatibility

object SparkVersionGuard {

  /**
   * Populates the version guard with the defaults from [[za.co.absa.enceladus.common.SparkCompatibility]]
   */
  def fromDefaultSparkCompatibilitySettings: SparkVersionGuard =
    SparkVersionGuard(SparkCompatibility.minSparkVersionIncluded, SparkCompatibility.maxSparkVersionExcluded)
}

/**
 * Setup Spark version guard with allowed min & max sem-ver version. Note that:
 *  - min version is included to be allowed and may be non-final
 *  - max version is excluded from allowed (supremum) and may be non-final
 *
 * @param minVersionInclusive
 * @param maxVersionExclusive
 */
case class SparkVersionGuard(minVersionInclusive: Version, maxVersionExclusive: Version) {

  /**
   * String wrapper for [[SparkVersionGuard#checkSparkVersionCompatibility(Version)]]
   *
   * @param yourVersion provided spark version
   */
  def checkSparkVersionCompatibility(yourVersion: String): Unit =
    checkSparkVersionCompatibility(Version.asSemVer(yourVersion))

  /**
   * Supplied version will be checked against the [[SparkVersionGuard]]'s. Note, `yourVersion` is
   * finalized when comparing to max in order to disallow non-final versions againt a final guard (3.0.0-rc.1
   * would be allowed when 3.0.0 is disallowed)
   *
   * @param yourVersion provided spark version
   */
  def checkSparkVersionCompatibility(yourVersion: Version): Unit = {
    import VersionExt._

    // `someVersion.finalVersion < maxExclusive` will guard against e.g. 3.0.0-rc.1 being allowed when 3.0.0 should not be
    assert(yourVersion >= minVersionInclusive && yourVersion.finalVersion < maxVersionExclusive,
      // todo use Version.asString for better error message!
      s"This SparkJob can only be ran on Spark version [$minVersionInclusive, $maxVersionExclusive) (min inclusive, max exclusive). " +
        s"Your detected version was $yourVersion.")
  }

}




