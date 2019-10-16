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

package za.co.absa.enceladus.menas

import org.springframework.context.annotation.{ Configuration, Bean }
import org.springframework.beans.factory.annotation.Value
import org.apache.oozie.client.OozieClient
import scala.util.Try
import za.co.absa.enceladus.menas.exceptions.OozieConfigurationException
import scala.util.Success
import scala.util.Failure
import org.apache.oozie.client.AuthOozieClient
import org.apache.oozie.client.AuthOozieClient.AuthType

@Configuration
class OozieConfig {

  @Value("${za.co.absa.enceladus.menas.oozie.oozieUrl:}")
  val oozieUrl: String = ""

  @Bean
  def oozieClient: Either[OozieConfigurationException, OozieClient] = {
    Try(new AuthOozieClient(oozieUrl, AuthType.KERBEROS.toString)) match {
      case Success(client) => Right(client)
      case Failure(e) => Left(OozieConfigurationException(e.getMessage, e))
    }
  }
}
