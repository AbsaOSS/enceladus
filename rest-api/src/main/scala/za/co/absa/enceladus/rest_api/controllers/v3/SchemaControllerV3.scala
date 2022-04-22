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

package za.co.absa.enceladus.rest_api.controllers.v3

import org.apache.spark.sql.types.StructType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import org.springframework.web.multipart.MultipartFile
import za.co.absa.enceladus.model.{Schema, Validation}
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.rest_api.controllers.SchemaController
import za.co.absa.enceladus.rest_api.exceptions.ValidationException
import za.co.absa.enceladus.rest_api.models.rest.exceptions.SchemaParsingException
import za.co.absa.enceladus.rest_api.repositories.RefCollection
import za.co.absa.enceladus.rest_api.services.v3.SchemaServiceV3
import za.co.absa.enceladus.rest_api.services.{AttachmentService, SchemaRegistryService, SchemaService}
import za.co.absa.enceladus.rest_api.utils.SchemaType
import za.co.absa.enceladus.rest_api.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.rest_api.utils.parsers.SchemaParser

import java.util.concurrent.CompletableFuture
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


@RestController
@RequestMapping(Array("/api-v3/schemas"))
class SchemaControllerV3 @Autowired()(
                                       schemaService: SchemaServiceV3,
                                       attachmentService: AttachmentService,
                                       sparkMenasConvertor: SparkMenasSchemaConvertor,
                                       schemaRegistryService: SchemaRegistryService
                                     )
  extends VersionedModelControllerV3(schemaService) {

  import za.co.absa.enceladus.rest_api.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @GetMapping(path = Array("/{name}/{version}/json"), produces = Array("application/json"))
  @ResponseStatus(HttpStatus.OK)
  def getJson(@PathVariable name: String,
              @PathVariable version: String,
              @RequestParam(defaultValue = "false") pretty: Boolean): CompletableFuture[String] = {
    forVersionExpression(name, version) (schemaService.getVersion).map {
      case Some(schema) =>
        if (schema.fields.isEmpty) throw ValidationException(
          Validation.empty.withError("schema-fields", s"Schema $name v$version exists, but has no fields!")
        )
        val sparkStruct = StructType(sparkMenasConvertor.convertMenasToSparkFields(schema.fields))
        if (pretty) sparkStruct.prettyJson else sparkStruct.json
      case None =>
        throw notFound()
    }

  }

  @GetMapping(path = Array("/{name}/{version}/original"))
  @ResponseStatus(HttpStatus.OK)
  def exportOriginalSchemaFile(@AuthenticationPrincipal principal: UserDetails,
                               @PathVariable name: String,
                               @PathVariable version: String,
                               response: HttpServletResponse): CompletableFuture[Array[Byte]] = {
    forVersionExpression(name, version)(attachmentService.getSchemaByNameAndVersion).map { attachment =>
        response.addHeader("mime-type", attachment.fileMIMEType)
        attachment.fileContent
    }
  }

  @PostMapping(Array("/{name}/{version}/from-file"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  def handleFileUpload(@AuthenticationPrincipal principal: UserDetails,
                       @PathVariable name: String,
                       @PathVariable version: Int,
                       @RequestParam file: MultipartFile,
                       @RequestParam format: String,
                       request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    val fileContent = new String(file.getBytes)

    val schemaType = SchemaType.fromSchemaName(format)
    val sparkStruct = SchemaParser.getFactory(sparkMenasConvertor).getParser(schemaType).parse(fileContent)

    // for avro schema type, always force the same mime-type to be persisted
    val mime = if (schemaType == SchemaType.Avro) {
      SchemaController.avscContentType
    } else {
      file.getContentType
    }

    val menasFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase,
      refName = name,
      refVersion = version + 1, // version is the current one, refVersion is the to-be-created one
      attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = file.getOriginalFilename,
      fileContent = file.getBytes,
      fileMIMEType = mime)

    uploadSchemaToMenas(principal.getUsername, menasFile, sparkStruct, schemaType).map { case (updatedSchema, validation) =>
      createdWithNameVersionLocationBuilder(name, updatedSchema.version, request,
        stripLastSegments = 3).body(validation)  // stripping: /{name}/{version}/from-file
    }
  }

  @PostMapping(Array("/{name}/{version}/from-remote-uri"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  def handleRemoteFile(@AuthenticationPrincipal principal: UserDetails,
                       @PathVariable name: String,
                       @PathVariable version: Int,
                       @RequestParam remoteUrl: String,
                       @RequestParam format: String,
                       request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    val schemaType = SchemaType.fromSchemaName(format)
    val schemaResponse = schemaRegistryService.loadSchemaByUrl(remoteUrl)
    val sparkStruct = SchemaParser.getFactory(sparkMenasConvertor).getParser(schemaType).parse(schemaResponse.fileContent)

    val menasFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase,
      refName = name,
      refVersion = version + 1,  // version is the current one, refVersion is the to-be-created one
      attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = schemaResponse.url.getFile,
      fileContent = schemaResponse.fileContent.getBytes,
      fileMIMEType = schemaResponse.mimeType)

    uploadSchemaToMenas(principal.getUsername, menasFile, sparkStruct, schemaType).map { case (updatedSchema, validation) =>
      createdWithNameVersionLocationBuilder(name, updatedSchema.version, request,
        stripLastSegments = 3).body(validation)  // stripping: /{name}/{version}/from-remote-uri
    }
  }

  @PostMapping(Array("/{name}/{version}/from-registry"))
  @ResponseStatus(HttpStatus.CREATED)
  @PreAuthorize("@authConstants.hasAdminRole(authentication)")
  def handleSubject(@AuthenticationPrincipal principal: UserDetails,
                    @PathVariable name: String,
                    @PathVariable version: Int,
                    @RequestParam subject: String,
                    @RequestParam format: String,
                    request: HttpServletRequest): CompletableFuture[ResponseEntity[Validation]] = {

    val schemaType = SchemaType.fromSchemaName(format)
    val valueSchemaResponse = Try {
      schemaRegistryService.loadSchemaBySubjectName(s"$subject")
    } match {
      case Success(schemaResponse) => schemaResponse
      case Failure(_) => schemaRegistryService.loadSchemaBySubjectName(s"$subject-value") // fallback to -value
    }

    val valueSparkStruct = SchemaParser.getFactory(sparkMenasConvertor).getParser(schemaType).parse(valueSchemaResponse.fileContent)

    val menasFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase,
      refName = name,
      refVersion = version + 1,  // version is the current one, refVersion is the to-be-created one
      attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = valueSchemaResponse.url.getFile, // only the value file gets saved as an attachment
      fileContent = valueSchemaResponse.fileContent.getBytes,
      fileMIMEType = valueSchemaResponse.mimeType)

    uploadSchemaToMenas(principal.getUsername, menasFile, valueSparkStruct, schemaType).map { case (updatedSchema, validation) =>
      createdWithNameVersionLocationBuilder(name, updatedSchema.version, request,
        stripLastSegments = 3).body(validation)  // stripping: /{name}/{version}/from-registry
    }
  }

  private def uploadSchemaToMenas(username: String, menasAttachment: MenasAttachment, sparkStruct: StructType,
                                  schemaType: SchemaType.Value): Future[(Schema, Validation)] = {
    try {
      for {
        // the parsing of sparkStruct can fail, therefore we try to save it first before saving the attachment
        (updated, validation) <- schemaService.schemaUpload(username, menasAttachment.refName, menasAttachment.refVersion - 1, sparkStruct)
        _ <- attachmentService.uploadAttachment(menasAttachment)
      } yield (updated, validation)
    } catch {
      case e: SchemaParsingException => throw e.copy(schemaType = schemaType) // adding schema type
    }
  }


}


