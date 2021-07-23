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

package za.co.absa.enceladus.plugins.builtin.common.mq.kafka

import com.typesafe.config.Config
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits

case class SchemaRegistrySecurityParams(credentialsSource: String,
                                        userInfo: Option[String])

object SchemaRegistrySecurityParams {
  val BasicAuthCredentialsSourceKey = "kafka.schema.registry.basic.auth.credentials.source"
  val BasicAuthUserInfoKey = "kafka.schema.registry.basic.auth.user.info"

  def fromConfig(conf: Config): Option[SchemaRegistrySecurityParams] = {
    conf.getOptionString(BasicAuthCredentialsSourceKey).map { source =>
      SchemaRegistrySecurityParams(source,
        conf.getOptionString(BasicAuthUserInfoKey))
    }
  }
}
