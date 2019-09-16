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

package za.co.absa.enceladus.dao

import com.typesafe.config.ConfigFactory
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.web.client.RestTemplate
import za.co.absa.enceladus.dao.menasplugin.MenasCredentials

object RestDaoFactory {

  private val conf = ConfigFactory.load()
  private val apiBaseUrl = conf.getString("menas.rest.uri")

  private val restTemplate = {
    val template = new RestTemplate()
    val converters = template.getMessageConverters
    converters.add(new MappingJackson2HttpMessageConverter(JsonSerializer.objectMapper))
    template.setMessageConverters(converters)
    template
  }

  def getInstance(authCredentials: MenasCredentials): MenasRestDAO = {
    val authClient = AuthClient(authCredentials, apiBaseUrl)
    new MenasRestDAO(apiBaseUrl, authClient, restTemplate)
  }

}
