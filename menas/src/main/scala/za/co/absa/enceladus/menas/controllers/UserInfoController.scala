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

package za.co.absa.enceladus.menas.controllers

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.core.GrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.user.UserInfo
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import org.springframework.security.web.csrf.CsrfToken

@RestController
@RequestMapping(Array("/api/user"))
class UserInfoController extends BaseController {

  import za.co.absa.enceladus.menas.utils.implicits._

  @Value("${za.co.absa.enceladus.menas.version}")
  val menasVersion: String = ""

  @GetMapping(path = Array("/info"))
  def userInfo(request: HttpServletRequest, response: HttpServletResponse): UserInfo = {
    val csrfToken = request.getAttribute("_csrf").asInstanceOf[CsrfToken]
    response.addHeader(csrfToken.getHeaderName, csrfToken.getToken)
    val auth = SecurityContextHolder.getContext().getAuthentication()
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]
    val groups = auth.getAuthorities.toArray(Array[GrantedAuthority]()).map(auth => auth.getAuthority)
    UserInfo(principal.getUsername, groups, menasVersion)
  }

  @GetMapping(path = Array("/version"))
  def getVersion(): String = {
    menasVersion
  }
}
