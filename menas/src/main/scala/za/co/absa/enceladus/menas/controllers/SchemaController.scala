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

import java.net.URL
import java.util.Optional
import java.util.concurrent.CompletableFuture

import javax.servlet.http.HttpServletResponse
import org.apache.spark.sql.types.StructType
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import org.springframework.web.multipart.MultipartFile
import za.co.absa.enceladus.menas.models.rest.exceptions.{RemoteSchemaRetrievalException, SchemaParsingException}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.menas.services.{AttachmentService, SchemaService}
import za.co.absa.enceladus.menas.utils.SchemaType
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.menas.utils.parsers.SchemaParser
import za.co.absa.enceladus.model.Schema
import za.co.absa.enceladus.model.menas._

import scala.io.Source
import scala.util.control.NonFatal

@RestController
@RequestMapping(Array("/api/schema"))
class SchemaController @Autowired()(
                                     schemaService: SchemaService,
                                     attachmentService: AttachmentService,
                                     sparkMenasConvertor: SparkMenasSchemaConvertor)
  extends VersionedModelController(schemaService) {

  import za.co.absa.enceladus.menas.utils.implicits._
  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(Array("/remote"))
  @ResponseStatus(HttpStatus.CREATED)
  def handleRemoteFile(@AuthenticationPrincipal principal: UserDetails,
                       @RequestParam remoteUrl: String,
                       @RequestParam version: Int,
                       @RequestParam name: String,
                       @RequestParam format: Optional[String]): CompletableFuture[Option[Schema]] = {

    val schemaType: SchemaType.Value = SchemaType.fromOptSchemaName(format)
    val (url, fileContent, mimeType) = {
      try {
        val url = new URL(remoteUrl)
        val connection = url.openConnection()
        val mimeType = SchemaController.avscConentType // only AVSC is expected to come from the schema registry
        val fileStream = Source.fromInputStream(connection.getInputStream)
        val fileContent = fileStream.mkString
        fileStream.close()

        (url, fileContent, mimeType)

      } catch {
        case NonFatal(e) =>
          throw RemoteSchemaRetrievalException(schemaType, s"Could not retrieve a schema file from $remoteUrl. Please check the correctness of the URL and a presence of the schema at the mentioned endpoint", e)
      }
    }

    val sparkStruct = SchemaParser.getFactory(sparkMenasConvertor).getParser(schemaType).parse(fileContent)

    val menasFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase,
      refName = name,
      refVersion = version + 1,  // version is the current one, refVersion is the to-be-created one
      attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = url.getFile,
      fileContent = fileContent.getBytes,
      fileMIMEType = mimeType)

    uploadSchemaToMenas(principal.getUsername, menasFile, sparkStruct, schemaType)
  }

  @PostMapping(Array("/upload"))
  @ResponseStatus(HttpStatus.CREATED)
  def handleFileUpload(@AuthenticationPrincipal principal: UserDetails,
                       @RequestParam file: MultipartFile,
                       @RequestParam version: Int,
                       @RequestParam name: String,
                       @RequestParam format: Optional[String]): CompletableFuture[Option[Schema]] = {

    val fileContent = new String(file.getBytes)

    val schemaType = SchemaType.fromOptSchemaName(format)
    val sparkStruct = SchemaParser.getFactory(sparkMenasConvertor).getParser(schemaType).parse(fileContent)

    // for avro schema type, always force the same mime-type to be persisted
    val mime = if (schemaType == SchemaType.Avro) {
     SchemaController.avscConentType
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

    uploadSchemaToMenas(principal.getUsername, menasFile, sparkStruct, schemaType)
  }

  /**
   * Common for [[SchemaController#handleFileUpload]] and [[SchemaController#handleRemoteFile]]
   */
  private def uploadSchemaToMenas(username: String, menasAttachment: MenasAttachment, sparkStruct: StructType,
                                  schemaType: SchemaType.Value): CompletableFuture[Option[Schema]] = {
    try {
      for {
        // the parsing of sparkStruct can fail, therefore we try to save it first before saving the attachment
        update <- schemaService.schemaUpload(username, menasAttachment.refName, menasAttachment.refVersion - 1, sparkStruct)
        _ <- attachmentService.uploadAttachment(menasAttachment)
      } yield update
    } catch {
      case e: SchemaParsingException => throw e.copy(schemaType = schemaType) //adding schema type
    }
  }

  @GetMapping(path = Array("/export/{name}/{version}"))
  @ResponseStatus(HttpStatus.OK)
  def exportSchema(@AuthenticationPrincipal principal: UserDetails,
                   @PathVariable name: String,
                   @PathVariable version: Int,
                   response: HttpServletResponse): CompletableFuture[Array[Byte]] = {
    attachmentService.getSchemaByNameAndVersion(name, version).map { attachment =>
      response.addHeader("mime-type", attachment.fileMIMEType)
      attachment.fileContent
    }
  }

  @GetMapping(path = Array("/json/{name}/{version}"), produces = Array("application/json"))
  @ResponseStatus(HttpStatus.OK)
  def getJson(@PathVariable name: String,
              @PathVariable version: Int,
              @RequestParam(defaultValue = "false") pretty: Boolean): CompletableFuture[String] = {
    schemaService.getVersion(name, version).map {
      case Some(schema) =>
        if (schema.fields.isEmpty) throw notFound()

        val sparkStruct = StructType(sparkMenasConvertor.convertMenasToSparkFields(schema.fields))
        if (pretty) sparkStruct.prettyJson else sparkStruct.json
      case None =>
        throw notFound()
    }
  }
}

object SchemaController {
  val avscConentType = "application/vnd.schemaregistry.v1+json"
}
