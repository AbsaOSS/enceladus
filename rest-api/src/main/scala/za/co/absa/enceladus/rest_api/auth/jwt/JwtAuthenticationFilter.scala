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

package za.co.absa.enceladus.rest_api.auth.jwt

import java.util

import io.jsonwebtoken.Claims
import javax.servlet.FilterChain
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.springframework.beans.factory.annotation.{Autowired, Value}
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.User
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import za.co.absa.enceladus.rest_api.auth.AuthConstants._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

@Component
class JwtAuthenticationFilter @Autowired()(jwtFactory: JwtFactory) extends OncePerRequestFilter {

  override def doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain): Unit = {
    getAuthentication(request)
      .foreach(SecurityContextHolder.getContext.setAuthentication)

    chain.doFilter(request, response)
  }

  private def getAuthentication(request: HttpServletRequest): Option[Authentication] = {
    getJwt(request).flatMap { jwt =>
      Try {
        jwtFactory
          .jwtParser()
          .parseClaimsJws(jwt)
          .getBody
      } match {
        case Failure(exception) =>
          logger.warn(s"JWT authentication failed $exception")
          None
        case Success(jwtClaims) => Option(parseJwtClaims(jwtClaims))
      }
    }
  }

  private def parseJwtClaims(jwtClaims: Claims): Authentication = {
    val username = jwtClaims.getSubject
    val authorities = parseJwtAuthorities(jwtClaims)
    val user = new User(username, "", authorities)

    new UsernamePasswordAuthenticationToken(user, jwtClaims, authorities)
  }

  private def parseJwtAuthorities(jwtClaims: Claims): util.List[SimpleGrantedAuthority] = {
    val groups: Seq[String] = Option(jwtClaims.get(RolesKey, classOf[util.List[String]]))
    .fold(Seq.empty[String])(_.asScala.toSeq)

    groups.map(k => new SimpleGrantedAuthority(k)).asJava
  }

  private def getJwt(request: HttpServletRequest): Option[String] = {
    val jwtHeader = request.getHeader(JwtKey)
    if(jwtHeader != null && jwtHeader.nonEmpty){
      Some(jwtHeader)
    } else None
  }

}
