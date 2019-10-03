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

package za.co.absa.enceladus.model.menas.jackson

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.core.JsonParser

class NonNullOptBooleanDeserializer extends StdDeserializer[Option[Boolean]](classOf[Option[Boolean]]) {
  override def deserialize(jsonParser: JsonParser, deserializationContext: DeserializationContext): Option[Boolean] = {
      Some(jsonParser.getBooleanValue)
  }

  override def getNullValue: Option[Boolean] = Some(false)
  override def getEmptyValue: Option[Boolean] = Some(false)
}
