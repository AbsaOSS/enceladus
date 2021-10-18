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

package za.co.absa.enceladus.menas.integration

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
      .setModifiable(actual.modifiable)
  }.asInstanceOf[T] // type of `expectedBase` will remain unchanged, good enough for testing support
}
