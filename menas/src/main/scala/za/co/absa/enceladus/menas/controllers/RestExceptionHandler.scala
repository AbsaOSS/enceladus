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

package za.co.absa.enceladus.menas.controllers

import org.apache.oozie.client.OozieClientException
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.http.converter.HttpMessageConversionException
import org.springframework.web.bind.annotation.{ControllerAdvice, ExceptionHandler, RestController}
import org.springframework.web.context.request.async.AsyncRequestTimeoutException
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException
import za.co.absa.enceladus.menas.exceptions._
import za.co.absa.enceladus.menas.models.RestError
import za.co.absa.enceladus.menas.models.rest.RestResponse
import za.co.absa.enceladus.menas.models.rest.errors.{RemoteSchemaRetrievalError, RequestTimeoutExpiredError, SchemaFormatError, SchemaParsingError}
import za.co.absa.enceladus.menas.models.rest.exceptions.{RemoteSchemaRetrievalException, SchemaFormatException, SchemaParsingException}
import za.co.absa.enceladus.model.{UsedIn, Validation}

@ControllerAdvice(annotations = Array(classOf[RestController]))
class RestExceptionHandler {

  @Value("${menas.oozie.customImpersonationExceptionMessage:}")
  val oozieImpersonationExceptionMessage: String = ""

  @Value("${menas.oozie.proxyGroup:}")
  val oozieProxyGroup: String = ""

  private val logger = LoggerFactory.getLogger(this.getClass)

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
    logger.error(s"HttpMessageConversionException: ${exception.getMessage}", exception)
    ResponseEntity.badRequest().body(exception.getMessage)
  }

  @ExceptionHandler(value = Array(classOf[EntityInUseException]))
  def handleValidationException(exception: EntityInUseException): ResponseEntity[UsedIn] = {
    ResponseEntity.badRequest().body(exception.usedIn)
  }

  @ExceptionHandler(value = Array(classOf[MethodArgumentTypeMismatchException]))
  def handleTypeMismatchException(exception: MethodArgumentTypeMismatchException): ResponseEntity[Any] = {
    ResponseEntity.notFound().build[Any]()
  }

  @ExceptionHandler(Array(classOf[OozieActionException]))
  def handleOozieActionException(ex: OozieActionException): ResponseEntity[RestError] = {
    val err = RestError(ex.getMessage)
    logger.error(s"Exception: $err", ex)
    new ResponseEntity(err, HttpStatus.INTERNAL_SERVER_ERROR)
  }

  @ExceptionHandler(Array(classOf[OozieClientException]))
  def handleOozieClientException(ex: OozieClientException): ResponseEntity[RestError] = {
    val err = if (ex.getMessage.toLowerCase.contains("unauthorized proxyuser")) {
      val message = if (oozieImpersonationExceptionMessage.nonEmpty) oozieImpersonationExceptionMessage else
        s"Please add the system user into ${oozieProxyGroup} group to use this feature."
      RestError(message)
    } else {
      RestError(ex.getMessage)
    }

    logger.error(s"Exception: $err", ex)
    new ResponseEntity(err, HttpStatus.INTERNAL_SERVER_ERROR)
  }
}
