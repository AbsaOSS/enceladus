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

package za.co.absa.enceladus.utils.implicits

import org.apache.spark.sql.types._
import scala.util.Try

object StructFieldImplicits {
  implicit class StructFieldEnhancements(val structField: StructField) {
    def getMetadataString(key: String): Option[String] = {
      Try(structField.metadata.getString(key)).toOption
    }

    def getMetadataChar(key: String): Option[Char] = {
      val resultString = Try(structField.metadata.getString(key)).toOption
      resultString.flatMap { s =>
        if (s.length == 1) {
          Option(s(0))
        } else {
          None
        }
      }
    }

    def getMetadataStringAsBoolean(key: String): Option[Boolean] = {
      Try(structField.metadata.getString(key).toBoolean).toOption
    }


    def hasMetadataKey(key: String): Boolean = {
      structField.metadata.contains(key)
    }
  }
}
