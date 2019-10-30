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

import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.web.client.RestTemplate

import scala.collection.JavaConverters._

object RestTemplateSingleton {

  val instance: RestTemplate = {
    val template = new RestTemplate()
    // Need to replace the default Jackson converter with one using a Scala serializer
    val converters = template.getMessageConverters.asScala
      .map {
        case _: MappingJackson2HttpMessageConverter => new MappingJackson2HttpMessageConverter(JsonSerializer.objectMapper)
        case converter                              => converter
      }.asJava
    template.setMessageConverters(converters)
    template.setErrorHandler(NoOpErrorHandler)
    template
  }

}
