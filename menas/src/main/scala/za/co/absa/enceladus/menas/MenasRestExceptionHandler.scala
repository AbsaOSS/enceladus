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

package za.co.absa.enceladus.menas

import org.springframework.core.annotation.Order
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler

import za.co.absa.enceladus.menas.models.RestError
import za.co.absa.enceladus.menas.exceptions.OozieActionException

@Order(org.springframework.core.Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice
class MenasRestExceptionHandler extends ResponseEntityExceptionHandler {

  @ExceptionHandler(Array(classOf[OozieActionException]))
   def handleOozieActionException(ex: RuntimeException): ResponseEntity[Object] = {
    val err = RestError(ex.getMessage)
    logger.error(s"Exception: $err\n${ex.getStackTrace.map(trace => s"\t\t${trace.toString}").mkString("\n")}")
    new ResponseEntity(err, HttpStatus.INTERNAL_SERVER_ERROR);
   }
}