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

package za.co.absa.enceladus

package object menas {

  /**
   * Resource paths of files for schema testing
   */
  object TestResourcePath {

    object Json {
      val ok = "/test_data/schemas/json/schema_json_ok.json"
      val bogus = "/test_data/schemas/json/schema_json_bogus.json"
    }

    object Copybook {
      val ok = "/test_data/schemas/copybook/copybook_ok.cob"
      val bogus = "/test_data/schemas/copybook/copybook_bogus.cob"
      val okJsonEquivalent = "/test_data/schemas/copybook/equivalent-to-copybook.json"
    }

    object Avro {
      val ok = "/test_data/schemas/avro/avroschema_json_ok.avsc"
      val bogus = "/test_data/schemas/avro/avroschema_json_bogus.avsc"
      val okJsonEquivalent = "/test_data/schemas/avro/equivalent-to-avroschema.json"
    }

    object AvroCombining {
      val value = "/test_data/schemas/avro_combine/avro-value.avsc"
      val key = "/test_data/schemas/avro_combine/avro-key.avsc"

      val expectedCombination = "/test_data/schemas/avro_combine/expected-combination.json"
    }

  }

}
