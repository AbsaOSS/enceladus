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

package za.co.absa.enceladus.model.versionedModel

/**
 * V2 Representation of `VersionedSummary` - V2 does not carry disabled information
 */
case class VersionedSummaryV2(_id: String, latestVersion: Int)

case class VersionedSummary(_id: String, latestVersion: Int, disabledSet: Set[Boolean]) {
  def toV2: VersionedSummaryV2 = VersionedSummaryV2(_id, latestVersion)

  def toNamedVersion: NamedVersion = {
    val disabled = disabledSet.contains(true) // legacy mixed state reported as disabled for V3 summary
    NamedVersion(_id, latestVersion, disabled)
  }
}


