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

package za.co.absa.enceladus.model

import com.fasterxml.jackson.annotation.JsonIgnore
import za.co.absa.enceladus.model.menas.MenasReference

case class UsedIn(datasets: Option[Seq[MenasReference]] = None,
                  mappingTables: Option[Seq[MenasReference]] = None) {

  /**
   * Should any of the original UsedIn equal to Some(Seq.empty), it will be None after normalization
   */
  @JsonIgnore
  lazy val normalized: UsedIn = {
    def normalizeOne(field: Option[Seq[MenasReference]]) = field match {
      case None => None
      case Some(x) if x.isEmpty => None
      case otherNonEmpty => otherNonEmpty
    }

    val normalizedDs = normalizeOne(datasets)
    val normalizedMt = normalizeOne(mappingTables)

    (normalizedDs, normalizedMt) match {
      case (`datasets`, `mappingTables`) => this // no normalization needed
      case _ => UsedIn(normalizedDs, normalizedMt)
    }
  }

  @JsonIgnore
  val isEmpty: Boolean = {
    normalized.datasets == None && normalized.mappingTables == None
  }

  @JsonIgnore
  val nonEmpty: Boolean = !isEmpty
}

object UsedIn {
  /**
   * Normalized
   */
  val empty = UsedIn(None, None)
}
