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

package za.co.absa.enceladus.rest_api.integration

import za.co.absa.enceladus.model.versionedModel.VersionedModel

package object controllers {
  // helper method to compare expected-actual objects disregarding some user/created properties
  private[controllers] def toExpected[T <: VersionedModel](expectedBase: T, actual: VersionedModel): T = {
    expectedBase
      .setDateCreated(actual.dateCreated)
      .setUserCreated(actual.userCreated)
      .setLastUpdated(actual.lastUpdated)
      .setUpdatedUser(actual.userUpdated)
      .setDateDisabled(actual.dateDisabled)
      .setUserDisabled(actual.userDisabled)
      .setLocked(actual.locked)
      .setDateLocked(actual.dateLocked)
      .setUserLocked(actual.userLocked)

  }.asInstanceOf[T] // type of `expectedBase` will remain unchanged, good enough for testing support

  trait TestPaginated[T] {
    def page: Array[T]
    def offset: Int
    def limit: Int
    def truncated: Boolean
  }

  trait CustomMatchers {

    import org.scalatest._
    import matchers._

    class TestPaginatedConformsMatcher[T](expectedPaginated: TestPaginated[T]) extends Matcher[TestPaginated[T]] {

      private def conforms(checked: TestPaginated[T]): Boolean = {
        checked.page.toSeq == expectedPaginated.page.toSeq &&
          checked.offset == expectedPaginated.offset &&
          checked.limit == expectedPaginated.limit &&
          checked.truncated == expectedPaginated.truncated
      }

      def apply(checked: TestPaginated[T]): MatchResult = {
        MatchResult(
          conforms(checked),
          s"""TestPaginated-check: $checked did not conform to "$expectedPaginated"""",
          s"""TestPaginated-check: $checked conformed to "$expectedPaginated""""
        )
      }
    }

    /**
     * TestPaginated contains pages in an Array, that cannot be compare by ==, this matcher circumvents that
     * @param expectedPagination
     * @tparam T
     * @return
     */
    def conformTo[T](expectedPagination: TestPaginated[T]): TestPaginatedConformsMatcher[T] = {
      new TestPaginatedConformsMatcher(expectedPagination)
    }
  }

  object CustomMatchers extends CustomMatchers
}
