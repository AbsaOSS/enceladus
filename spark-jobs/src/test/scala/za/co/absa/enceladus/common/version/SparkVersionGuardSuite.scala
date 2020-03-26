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

import org.scalatest.{Assertion, FlatSpec, Matchers}
import za.co.absa.commons.version.Version._

import scala.reflect.ClassTag

class SparkVersionGuardSuite extends FlatSpec with Matchers {

  private def ensureThrowsWithMessageIncluding[T <: Throwable](messageSubstringToAppear: String)(fun: => scala.Any)
                                                              (implicit ev: ClassTag[T]): Assertion = {
    val caught = the[T] thrownBy (fun)
    caught.getMessage should include(messageSubstringToAppear)
  }

   // Specific checks for the [[SparkVersionGuard]]
  private def ensureFailsVersionTooLow =
    ensureThrowsWithMessageIncluding[AssertionError]("Your Spark version is too low.") _

  private def ensureFailsVersionTooHigh =
    ensureThrowsWithMessageIncluding[AssertionError]("Your Spark version is too high.") _


  "SparkVersionGuard" should "check basic version compatibility" in {
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"2.4.5")
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"2.4.4") // min is inclusive
    SparkVersionGuard(semver"1.6.0", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"2.4.4")

    SparkVersionGuard(semver"1.6.0", None).ensureSparkVersionCompatibility(semver"2.4.4")

    ensureFailsVersionTooLow {
      SparkVersionGuard(semver"2.4.0", None).ensureSparkVersionCompatibility(semver"2.3.1") // below min
    }

    ensureFailsVersionTooHigh {
      SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"3.0.0") // max is exclusive
    }

    ensureFailsVersionTooHigh {
      SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"3.1.0") // above max
    }
  }

  it should "handle some special cases, too" in {
    // allow non-final in-between
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"2.5.0-alpha.beta-2")
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"2.5.0+20130313144700") // bigInt suffix

    // non-final guards can be used
    SparkVersionGuard(semver"2.4.0-milestone.6", Some(semver"3.0.2-rc.9")).ensureSparkVersionCompatibility(semver"3.0.1")

    ensureFailsVersionTooHigh {
      SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"3.0.0-alpha") // do not allow 3.x
    }

    ensureFailsVersionTooLow {
      SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility(semver"2.4.4-rc.7") // pre-min version
    }

  }

  it should "work with strings, too" in {
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility("2.5.1")
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility("2.5.1-SNAPSHOT")
    SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility("2.5.1-dev+99")
    SparkVersionGuard(semver"2.4.4", None).ensureSparkVersionCompatibility("3.4.5")

    val caughtBogus1 = intercept[IllegalArgumentException] {
      SparkVersionGuard(semver"2.4.4", Some(semver"3.0.0")).ensureSparkVersionCompatibility("bogus")
    }
    caughtBogus1.getMessage should include("does not correspond to the SemVer")

  }

}
