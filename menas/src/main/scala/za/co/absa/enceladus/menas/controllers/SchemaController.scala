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
import za.co.absa.enceladus.menas.models.rest.exceptions.{SchemaFormatException, SchemaParsingException}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.menas.services.{AttachmentService, SchemaService}
import za.co.absa.enceladus.menas.utils.SchemaParsers
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.model.Schema
import za.co.absa.enceladus.model.menas._

object SchemaType extends Enumeration {
  val Struct = Value("struct")
  val Copybook = Value("copybook")
  val Avro = Value("avro")

  /**
   * Conversion from a sting-based name to [[SchemaType.Value]] that throws correct [[SchemaFormatException]] on error.
   * Basically, this is [[SchemaType#withName]] with exception wrapping
   *
   * @param schemaName
   * @return converted schema format
   * @throws SchemaFormatException if a schema not resolvable by [[SchemaType#withName]] is supplied
   */
  def fromSchemaName(schemaName: String): SchemaType.Value =
    try {
      SchemaType.withName(schemaName)
    } catch {
      case e: NoSuchElementException =>
        throw SchemaFormatException(schemaName, s"'$schemaName' is not a recognized schema format. " +
          s"Menas currently supports: ${SchemaType.values.mkString(", ")}.", cause = e)
    }
}

@RestController
@RequestMapping(Array("/api/schema"))
class SchemaController @Autowired()(
                                     schemaService: SchemaService,
                                     attachmentService: AttachmentService,
                                     sparkMenasConvertor: SparkMenasSchemaConvertor,
                                     schemaParsers: SchemaParsers)
  extends VersionedModelController(schemaService) {

  import SchemaType._
  import za.co.absa.enceladus.menas.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(Array("/upload"))
  @ResponseStatus(HttpStatus.CREATED)
  def handleFileUpload(@AuthenticationPrincipal principal: UserDetails,
                       @RequestParam file: MultipartFile,
                       @RequestParam version: Int,
                       @RequestParam name: String,
                       @RequestParam format: Optional[String]): CompletableFuture[Option[Schema]] = {

    val fileContent = new String(file.getBytes)
    val origFormat: Option[String] = format

    val (sparkStruct, schemaType) = origFormat match {
      case Some("") | None => (schemaParsers.parseStructType(fileContent), SchemaType.Struct) // legacy compatibility cases
      case Some(explicitSchemaName) =>
        SchemaType.fromSchemaName(explicitSchemaName) match {
          case Struct => (schemaParsers.parseStructType(fileContent), Struct)
          case Copybook => (schemaParsers.parseCopybook(fileContent), Copybook)
          case Avro => (schemaParsers.parseAvro(fileContent), Avro)
        }
    }

    val origFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase,
      refName = name,
      refVersion = version + 1,
      attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = file.getOriginalFilename,
      fileContent = file.getBytes,
      fileMIMEType = file.getContentType)

    try {

      for {
        // the parsing of sparkStruct can fail, therefore we try to save it first before saving the attachment
        update <- schemaService.schemaUpload(principal.getUsername, name, version, sparkStruct)
        _ <- attachmentService.uploadAttachment(origFile)
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
