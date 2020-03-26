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
import za.co.absa.commons.version.impl.SemVer20Impl.SemanticVersion
import za.co.absa.enceladus.common.SparkCompatibility

object SparkVersionGuard {

  /**
   * Populates the version guard with the defaults from [[za.co.absa.enceladus.common.SparkCompatibility]]
   */
  def fromDefaultSparkCompatibilitySettings: SparkVersionGuard =
    SparkVersionGuard(SparkCompatibility.minSparkVersionIncluded, SparkCompatibility.maxSparkVersionExcluded)
}

/**
 * Setup Spark version guard with allowed min & max sem-ver version.
 *
 * @param minVersionInclusive lowest acceptable spark version (may be non-final)
 * @param maxVersionExclusive version supremum - first disallowed spark version (this and higher cannot be used)
 */
case class SparkVersionGuard(minVersionInclusive: SemanticVersion, maxVersionExclusive: SemanticVersion) {

  /**
   * String wrapper for [[SparkVersionGuard#checkSparkVersionCompatibility(SemanticVersion)]]
   *
   * @param yourVersion provided spark version
   */
  def ensureSparkVersionCompatibility(yourVersion: String): Unit =
    ensureSparkVersionCompatibility(Version.asSemVer(yourVersion))

  /**
   * Supplied version will be checked against the [[SparkVersionGuard]]'s. Note, `yourVersion` is
   * finalized when comparing to max in order to disallow non-final versions against a final guard (3.0.0-rc.1
   * would be allowed when 3.0.0 is disallowed)
   *
   * @param yourVersion provided spark version
   */
  def ensureSparkVersionCompatibility(yourVersion: SemanticVersion): Unit = {

    // `someVersion.core < maxExclusive` guards against e.g. 3.0.0-rc.1 being allowed when 3.0.0 should not be
    assert(yourVersion >= minVersionInclusive && yourVersion.core < maxVersionExclusive,
      // scalastyle:off line.size.limit string clearer in one line
      s"""This SparkJob can only run on Spark version [${minVersionInclusive.asString}., ${maxVersionExclusive.asString}) (min inclusive, max exclusive).
         |Your detected version was ${yourVersion.asString}""".stripMargin)
    // scalastyle:on
  }

}


