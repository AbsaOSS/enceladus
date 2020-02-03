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

package za.co.absa.enceladus.menas.models.rest

import java.time.ZonedDateTime
import java.util.UUID

/**
  * A class for returning uniform REST responses in a form:
  * {{{
  * {
  *   "message": "Authentication required before accessing this page.",
  *   "timespamp": "2019-09-02T11:54:45.574Z",
  *   "id": "67a8b19b-f88c-4792-ba2a-2d67ec1a74b9",
  *   "error": {
  *     "error_type": "missing_authentication",
  *     ...
  *   }
  * }
  * }}}
  */
case class RestResponse(
                         message: String,
                         error: Option[ResponseError] = None,
                         timestamp: ZonedDateTime = ZonedDateTime.now(),
                         id: UUID = UUID.randomUUID()
                       )
