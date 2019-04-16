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

import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.{ControllerAdvice, ExceptionHandler, RestController}
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import za.co.absa.enceladus.model.UsedIn
import za.co.absa.enceladus.menas.exceptions.{EntityInUseException, NotFoundException, ValidationException}
import za.co.absa.enceladus.menas.models.Validation

@ControllerAdvice(annotations = Array(classOf[RestController]))
class RestExceptionHandler {

  @ExceptionHandler(value = Array(classOf[NotFoundException]))
  def handleNotFoundException(exception: NotFoundException): ResponseEntity[Any] = {
    ResponseEntity.notFound().build[Any]()
  }

  @ExceptionHandler(value = Array(classOf[ValidationException]))
  def handleValidationException(exception: ValidationException): ResponseEntity[Validation] = {
    ResponseEntity.badRequest().body(exception.validation)
  }

  @ExceptionHandler(value = Array(classOf[EntityInUseException]))
  def handleValidationException(exception: EntityInUseException): ResponseEntity[UsedIn] = {
    ResponseEntity.badRequest().body(exception.usedIn)
  }

  @ExceptionHandler(value = Array(classOf[MethodArgumentTypeMismatchException]))
  def handleTypeMismatchException(exception: MethodArgumentTypeMismatchException): ResponseEntity[Any] = {
    ResponseEntity.notFound().build[Any]()
  }

}
