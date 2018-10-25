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

package za.co.absa.enceladus.rest.controllers

import org.springframework.beans.factory.annotation.Value
import org.springframework.security.core.GrantedAuthority
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import za.co.absa.enceladus.model.user.UserInfo

@RestController 
@RequestMapping(Array("/api/user"))
class UserInfoController {

  import za.co.absa.enceladus.rest.utils.implicits._

  @Value("${za.co.absa.enceladus.menas.version}")
  val menasVersion: String = ""

  @GetMapping(path = Array("/info"))
  def userInfo() = {
    val auth = SecurityContextHolder.getContext().getAuthentication()
    val principal = auth.getPrincipal.asInstanceOf[UserDetails]
    val groups = auth.getAuthorities.toArray(Array[GrantedAuthority]()).map(auth => auth.getAuthority).filter(_.startsWith("f"))
    UserInfo(principal.getUsername, groups, menasVersion)
  }
  

}