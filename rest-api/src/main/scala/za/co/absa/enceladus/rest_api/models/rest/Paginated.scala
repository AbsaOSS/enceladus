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

package za.co.absa.enceladus.rest_api.models.rest

case class Paginated[T](page: Seq[T], offset:Int, limit: Int, truncated: Boolean)

object Paginated {

  /**
   * Wraps data in Paginated and possibly truncates the data to fit the limit. `truncated` flag is set based on that.
   * @param data data to be embedded and truncated to length of `limit`
   * @param offset is only passed to the wrapper object, has no effect to data
   * @param limit passed to the wrapper object and it used for truncating the data
   * @tparam T
   * @return truncated data wrapped in Paginated - `truncated` is true if data truncated, false otherwise.
   */
  def truncateToPaginated[T](data: Seq[T], offset: Int, limit: Int): Paginated[T] = {
    if (data.length <= limit) {
      Paginated(data, offset, limit, truncated = false)
    } else {
      Paginated(data.take(limit), offset, limit, truncated = true)
    }

  }
}
