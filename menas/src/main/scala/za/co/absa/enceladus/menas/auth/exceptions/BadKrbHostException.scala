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

package za.co.absa.enceladus.menas.auth.exceptions

import org.springframework.security.core.AuthenticationException

/**
  * This exception is thrown when Menas failed to communicate to a KDC.
  *
  * It needs to extend [[AuthenticationException]] so it could be processed by an authentication failure handler
  * to return the proper status code in the HTTP response.
  */
final case class BadKrbHostException(private val message: String,
                                     private val cause: Throwable = None.orNull)
  extends AuthenticationException(message, cause)
