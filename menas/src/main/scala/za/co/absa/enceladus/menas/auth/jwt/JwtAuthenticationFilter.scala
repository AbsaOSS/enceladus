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

import java.util

import io.jsonwebtoken.{Claims, JwtParser}
import javax.servlet.FilterChain
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.User
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import za.co.absa.enceladus.menas.auth.AuthConstants._

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
    getJwtCookie(request).flatMap { jwt =>
      Try {
        jwtFactory
          .jwtParser()
          .parseClaimsJws(jwt)
          .getBody
      } match {
        case Failure(exception) =>
          logger.warn(s"JWT authentication failed $exception")
          None
        case Success(jwtClaims) =>
          if (isCsrfSafe(request, jwtClaims)) {
            Option(parseJwtClaims(jwtClaims))
          } else {
            logger.warn("JWT authentication failed: Incorrect CSRF token")
            None
          }
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
    val groups = Option(jwtClaims.get(GroupsKey, classOf[util.List[String]]))
      .getOrElse(new util.ArrayList[String]()).asScala

    groups.map(k => new SimpleGrantedAuthority(k)).asJava
  }

  private def isCsrfSafe(request: HttpServletRequest, jwtClaims: Claims): Boolean = {
    request.getMethod.toUpperCase match {
      case "GET" => true
      case _     =>
        val csrfToken = Option(request.getHeader(CsrfTokenKey))
        val jwtCsrfToken = jwtClaims.get(CsrfTokenKey, classOf[String])
        csrfToken.contains(jwtCsrfToken)
    }

  }

  private def getJwtCookie(request: HttpServletRequest): Option[String] = {
    Option(request.getCookies).getOrElse(Array()).collectFirst {
      case cookie if cookie.getName == JwtCookieKey => cookie.getValue
    }
  }

}
