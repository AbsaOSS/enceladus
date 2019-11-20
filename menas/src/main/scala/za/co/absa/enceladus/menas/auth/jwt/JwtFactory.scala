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

package za.co.absa.enceladus.menas.auth.jwt

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.jsonwebtoken.io.{JacksonDeserializer, JacksonSerializer}
import io.jsonwebtoken.security.Keys
import io.jsonwebtoken.{JwtBuilder, JwtParser, Jwts}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.context.annotation.Bean
import org.springframework.stereotype.Component

@Component
class JwtFactory @Autowired()(@Value("${za.co.absa.enceladus.menas.auth.signing-key}")
                             signingKey: String) {

  private lazy val secretKey = Keys.hmacShaKeyFor(signingKey.getBytes)

  private val objectMapper = new ObjectMapper()
    .registerModule(new JavaTimeModule())
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  @Bean
  def jwtParser(): JwtParser = {
    Jwts.parser
      .setSigningKey(secretKey)
      .deserializeJsonWith(new JacksonDeserializer(objectMapper))
  }

  @Bean
  def jwtBuilder(): JwtBuilder = {
    Jwts.builder()
      .signWith(secretKey)
      .serializeToJsonWith(new JacksonSerializer(objectMapper))
  }

}
