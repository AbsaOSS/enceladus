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

package za.co.absa.enceladus.rest_api.integration.controllers

import za.co.absa.enceladus.model.versionedModel.NamedVersion
import za.co.absa.enceladus.rest_api.controllers.v3.PaginatedController
import za.co.absa.enceladus.rest_api.models.RunSummary
import za.co.absa.enceladus.rest_api.models.rest.Paginated

package object v3 {

  case class TestPaginatedNamedVersion(page: Array[NamedVersion],
                                       offset: Int = PaginatedController.DefaultOffset,
                                       limit: Int = PaginatedController.DefaultLimit,
                                       truncated: Boolean = false) extends TestPaginated[NamedVersion] {

    override def toString: String = {
      // just to print nicely, not directly used for comparisons
      s"TestPaginatedNamedVersion(page=${page.mkString("[", ",", "]")},offset=$offset,limit=$limit,truncated=$truncated)"
    }
  }

  implicit class PaginatedExtNamedVersion(val expected: Paginated[NamedVersion]) extends AnyVal {
    def asTestPaginated: TestPaginatedNamedVersion = {
      TestPaginatedNamedVersion(expected.page.toArray, expected.offset, expected.limit, expected.truncated)
    }
  }

  case class TestPaginatedRunSummary(page: Array[RunSummary],
                                     offset: Int = PaginatedController.DefaultOffset,
                                     limit: Int = PaginatedController.DefaultLimit,
                                     truncated: Boolean = false) extends TestPaginated[RunSummary] {

    override def toString: String = {
      // just to print nicely, not directly used for comparisons
      s"TestPaginatedRunSummary(page=${page.mkString("[", ",", "]")},offset=$offset,limit=$limit,truncated=$truncated)"
    }
  }

  implicit class PaginatedExtRunSummary(val expected: Paginated[RunSummary]) extends AnyVal {
    def asTestPaginated: TestPaginatedRunSummary = {
      TestPaginatedRunSummary(expected.page.toArray, expected.offset, expected.limit, expected.truncated)
    }
  }

}
