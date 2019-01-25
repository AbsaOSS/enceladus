/*
 * Copyright 2018-2019 ABSA Group Limited
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

package za.co.absa.enceladus.rest.models

case class ChangedField[T](fieldName: String,
    value1: T,
    value2: T)

case class ChangedFieldsUpdateTransformResult[C](updatedEntity: C,
    fields: Seq[ChangedField[Any]]) {

  /**
   * Generates list of fields, for which two values do not match - i.e have been changed
   * @return A list of fields for which values do not match
   */
  def getUpdatedFields(): Seq[String] = {
    fields.filter({ case field => field.value1 != field.value2 }).map(_.fieldName).toSeq
  }
}
