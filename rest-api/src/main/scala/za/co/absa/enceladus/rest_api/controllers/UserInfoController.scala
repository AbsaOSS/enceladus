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

package za.co.absa.enceladus.rest_api.controllers

import io.swagger.v3.oas.annotations.media.{Content, Schema => AosSchema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.security.SecurityRequirement
import org.springframework.http.HttpStatus
import org.springframework.security.core.GrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.user.UserInfo
import za.co.absa.enceladus.utils.general.ProjectMetadata

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

@RestController
@RequestMapping(Array("/api/user"))
class UserInfoController extends BaseController with ProjectMetadata {

  @SecurityRequirement(name = "JWT")
  @ApiResponse(responseCode = "401", description = "Unauthorized", content = Array(new Content(schema = new AosSchema()))) // no content
  @ResponseStatus(HttpStatus.OK)
  @GetMapping(path = Array("/info"))
  def userInfo(request: HttpServletRequest, response: HttpServletResponse): UserInfo = {
    val auth = SecurityContextHolder.getContext.getAuthentication
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]
    val groups = auth.getAuthorities.toArray(Array[GrantedAuthority]()).map(auth => auth.getAuthority)
    UserInfo(principal.getUsername, groups, projectVersion)
  }

  @GetMapping(path = Array("/version"))
  def getVersion(): String = {
    projectVersion
  }
}
