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

package za.co.absa.enceladus.conformance.interpreter

sealed trait ThreeStateSwitch {
  val name: String
}

object ThreeStateSwitch {
  def apply(value: String): ThreeStateSwitch = value match {
    case Always.name => Always
    case Never.name  => Never
    case Auto.name   => Auto
    case str         => throw new IllegalArgumentException(s"Unknown switch: '$str'. It should be one of: 'always', 'never', 'auto'.")
  }
}

case object Always extends ThreeStateSwitch {
  val name = "always"
}

case object Never extends ThreeStateSwitch {
  val name = "never"
}

case object Auto extends ThreeStateSwitch {
  val name = "auto"
}
