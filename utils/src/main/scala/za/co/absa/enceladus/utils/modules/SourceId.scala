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

package za.co.absa.enceladus.utils.modules

sealed trait SourceId {
  val value: String

  def asIdentifier: String = value.toLowerCase
}

object SourceId {
  def withIdentifier(name: String): SourceId = {
    name match {
      case "conformance" => SourceId.Conformance
      case "standardization" => SourceId.Standardization
      case _ => throw new NoSuchElementException(s"No value found for '$name'")
    }
  }

  case object Standardization extends SourceId {
    val value = "Standardization"
  }

  case object Conformance extends SourceId {
    val value = "Conformance"
  }
}
