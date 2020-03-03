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

package za.co.absa.enceladus.plugins.builtin.kafka

import com.typesafe.config.Config

case class KafkaSecurityParams(
                                securityProtocol: String,
                                saslMechanism: Option[String]
                              )

object KafkaSecurityParams {
  val SecurityProtocolKey = "kafka.security.protocol"
  val SaslMechanismKey = "kafka.sasl.mechanism"

  def fromConfig(conf: Config): Option[KafkaSecurityParams] = {
    if (conf.hasPath(SecurityProtocolKey)) {
      Option(KafkaSecurityParams(conf.getString(SecurityProtocolKey),
        getOptionConf(conf, SaslMechanismKey)))
    } else {
      None
    }
  }

  private def getOptionConf(conf: Config, key: String): Option[String] = {
    if (conf.hasPath(key)) {
      Option(conf.getString(key))
    } else {
      None
    }
  }
}
