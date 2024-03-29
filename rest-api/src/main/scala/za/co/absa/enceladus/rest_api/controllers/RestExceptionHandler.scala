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

import com.fasterxml.jackson.databind.JsonMappingException
import org.slf4j.LoggerFactory
import org.springframework.http.converter.HttpMessageConversionException
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.web.bind.annotation.{ControllerAdvice, ExceptionHandler, RestController}
import org.springframework.web.context.request.async.AsyncRequestTimeoutException
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import za.co.absa.enceladus.model.Validation
import za.co.absa.enceladus.model.properties.propertyType.PropertyTypeValidationException
import za.co.absa.enceladus.rest_api.exceptions._
import za.co.absa.enceladus.rest_api.models.rest.RestResponse
import za.co.absa.enceladus.rest_api.models.rest.errors.{RemoteSchemaRetrievalError, RequestTimeoutExpiredError, SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.rest_api.models.rest.exceptions.{RemoteSchemaRetrievalException, SchemaFormatException, SchemaParsingException}

@ControllerAdvice(annotations = Array(classOf[RestController]))
class RestExceptionHandler {

  private val logger = LoggerFactory.getLogger(this.getClass)

  @ExceptionHandler(value = Array(classOf[IllegalArgumentException]))
  def handleIllegalArgumentException(exception: IllegalArgumentException): ResponseEntity[Any] = {
    ResponseEntity.badRequest().body(exception.getMessage)
  }

  @ExceptionHandler(value = Array(classOf[AsyncRequestTimeoutException]))
  def handleAsyncRequestTimeoutException(exception: AsyncRequestTimeoutException): ResponseEntity[Any] = {
    val message = Option(exception.getMessage).getOrElse("Request timeout expired.")
    val response = RestResponse(message, Option(RequestTimeoutExpiredError()))
    logger.error(s"Exception: $response", exception)
    ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response)
  }

  @ExceptionHandler(value = Array(classOf[NotFoundException]))
  def handleNotFoundException(exception: NotFoundException): ResponseEntity[Any] = {
    ResponseEntity.notFound().build[Any]()
  }

  @ExceptionHandler(value = Array(classOf[SchemaParsingException]))
  def handleBadRequestException(exception: SchemaParsingException): ResponseEntity[Any] = {
    val response = RestResponse(exception.message, Option(SchemaParsingError.fromException(exception)))
    logger.error(s"Exception: $response", exception)
    ResponseEntity.badRequest().body(response)
  }

  @ExceptionHandler(value = Array(classOf[SchemaFormatException]))
  def handleBadRequestException(exception: SchemaFormatException): ResponseEntity[Any] = {
    val response = RestResponse(exception.message, Option(SchemaFormatError.fromException(exception)))
    logger.error(s"Exception: $response", exception)
    ResponseEntity.badRequest().body(response)
  }

  @ExceptionHandler(value = Array(classOf[RemoteSchemaRetrievalException]))
  def handleBadRequestException(exception: RemoteSchemaRetrievalException): ResponseEntity[Any] = {
    val response = RestResponse(exception.message, Option(RemoteSchemaRetrievalError.fromException(exception)))
    logger.error(s"Exception: $response", exception)
    ResponseEntity.badRequest().body(response)
  }

  @ExceptionHandler(value = Array(classOf[ValidationException]))
  def handleValidationException(exception: ValidationException): ResponseEntity[Validation] = {
    ResponseEntity.badRequest().body(exception.validation)
  }

  // when json <-> object mapping fails, respond with 400 instead of 500
  @ExceptionHandler(value = Array(classOf[HttpMessageConversionException]))
  def handleHttpMessageConversionException(exception: HttpMessageConversionException): ResponseEntity[Any] = {

    // logic: the cause may be our custom PropertyTypeValidationException or another general exception
    val specificMessage = exception.getCause match {
      case jme:JsonMappingException => jme.getCause match {
        case ptve:PropertyTypeValidationException => ptve.getMessage
        case _ => jme.getMessage
      }
      case _ => exception.getMessage
    }

    logger.error(s"HttpMessageConversionException: $specificMessage", exception)
    ResponseEntity.badRequest().body(specificMessage)
  }

  @ExceptionHandler(value = Array(classOf[EntityInUseException]))
  def handleValidationException(exception: EntityInUseException): ResponseEntity[EntityInUseException] = {
    ResponseEntity.badRequest().body(exception)
  }

  @ExceptionHandler(value = Array(classOf[LockedEntityException]))
  def handleValidationException(exception: LockedEntityException): ResponseEntity[String] = {
    ResponseEntity.status(HttpStatus.FORBIDDEN).body(exception.message)
  }

  @ExceptionHandler(value = Array(classOf[MethodArgumentTypeMismatchException]))
  def handleTypeMismatchException(exception: MethodArgumentTypeMismatchException): ResponseEntity[Any] = {
    val queryStringParamNames = Set("validateProperties", "forRun")
    if (queryStringParamNames.contains(exception.getName)) {
      ResponseEntity.badRequest().body(s"Unrecognized value '${exception.getValue}' for parameter `${exception.getName}`")
    }else {
      ResponseEntity.notFound().build[Any]()
    }
  }
}
