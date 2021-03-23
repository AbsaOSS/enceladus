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
import za.co.absa.enceladus.plugins.builtin.common.mq.kafka.KafkaSecurityParams.{SaslMechanismKey, SecurityProtocolKey}
import za.co.absa.enceladus.utils.config.ConfigUtils.ConfigImplicits

/**
 * This case class contains security parameters for Kafka secure connections.
 */
case class KafkaSecurityParams(
                                securityProtocol: String,
                                saslMechanism: Option[String]
                              ) {
  def toMap: Map[String, String] = {
    Map(SecurityProtocolKey -> securityProtocol) ++
      saslMechanism.fold(Map.empty[String, String])(saslValue => Map(SaslMechanismKey -> saslValue))
  }
}

object KafkaSecurityParams {
  val SecurityProtocolKey = "kafka.security.protocol"
  val SaslMechanismKey = "kafka.sasl.mechanism"

  /**
   * Parses configuration and returns kafka security properties.
   * If no security protocol provided an unsecured Kafka connection is assumed.
   *
   * @param conf A configuration.
   * @return An instance of Kafka security parameters.
   */
  def fromConfig(conf: Config): Option[KafkaSecurityParams] = {
    conf.getOptionString(SecurityProtocolKey).map { securityProtocol =>
      KafkaSecurityParams(securityProtocol,
        conf.getOptionString(SaslMechanismKey))
    }
  }

}
