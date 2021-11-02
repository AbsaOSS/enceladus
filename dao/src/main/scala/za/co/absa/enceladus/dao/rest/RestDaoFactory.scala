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

package za.co.absa.enceladus.dao.rest

import za.co.absa.enceladus.dao.auth.MenasCredentials
import za.co.absa.enceladus.dao.rest.RestDaoFactory.AvailabilitySetup.{Fallback, AvailabilitySetup, RoundRobin}

object RestDaoFactory {

  object AvailabilitySetup extends Enumeration {
    final type AvailabilitySetup = Value

    final val RoundRobin = Value("roundrobin")
    final val Fallback = Value("fallback")
  }

  final val DefaultAvailabilitySetup: AvailabilitySetup = RoundRobin

  private val restTemplate = RestTemplateSingleton.instance

  def getInstance(authCredentials: MenasCredentials,
                  apiBaseUrls: List[String],
                  urlsRetryCount: Option[Int] = None,
                  menasSetup: AvailabilitySetup = DefaultAvailabilitySetup): MenasRestDAO = {
    val startsWith = if (menasSetup == Fallback) {
      Option(0)
    } else {
      None
     }
    val apiCaller = CrossHostApiCaller(apiBaseUrls, urlsRetryCount.getOrElse(CrossHostApiCaller.DefaultUrlsRetryCount), startsWith)
    val authClient = AuthClient(authCredentials, apiCaller)
    val restClient = new RestClient(authClient, restTemplate)
    new MenasRestDAO(apiCaller, restClient)
  }
}
