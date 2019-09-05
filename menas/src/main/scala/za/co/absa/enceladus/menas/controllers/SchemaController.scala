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
import za.co.absa.cobrix.cobol.parser.CopybookParser
import za.co.absa.cobrix.cobol.parser.exceptions.SyntaxErrorException
import za.co.absa.cobrix.spark.cobol.schema.{CobolSchema, SchemaRetentionPolicy}
import za.co.absa.enceladus.menas.models.rest.exceptions.{SchemaFormatException, SchemaParsingException}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.menas.services.{AttachmentService, SchemaService}
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.model.Schema
import za.co.absa.enceladus.model.menas._

@RestController
@RequestMapping(Array("/api/schema"))
class SchemaController @Autowired() (
  schemaService:     SchemaService,
  attachmentService: AttachmentService,
  sparkMenasConvertor: SparkMenasSchemaConvertor)
  extends VersionedModelController(schemaService) {

  import za.co.absa.enceladus.menas.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  val SchemaTypeStruct = "struct"
  val SchemaTypeCopybook = "copybook"

  @PostMapping(Array("/upload"))
  @ResponseStatus(HttpStatus.CREATED)
  def handleFileUpload(@AuthenticationPrincipal principal: UserDetails,
                       @RequestParam file: MultipartFile,
                       @RequestParam version: Int,
                       @RequestParam name: String,
                       @RequestParam format: Optional[String]): CompletableFuture[Option[Schema]] = {
    val origFormat: Option[String] = format
    val fileContent = new String(file.getBytes)

    val sparkStruct = origFormat match {
      case Some(SchemaTypeStruct) | Some("") | None => parseStructType(fileContent)
      case Some(SchemaTypeCopybook) => parseCopybook(fileContent)
      case Some(schemaType) =>
        throw SchemaFormatException(schemaType, s"'$schemaType' is not a recognized schema format. " +
          s"Menas currently supports: '$SchemaTypeStruct' and '$SchemaTypeCopybook'.")
    }

    val origFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase,
      refName = name,
      refVersion = version + 1,
      attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = file.getOriginalFilename,
      fileContent = file.getBytes,
      fileMIMEType = file.getContentType)

    for {
      upload <- attachmentService.uploadAttachment(origFile)
      update <- schemaService.schemaUpload(principal.getUsername, name, version, sparkStruct)
    } yield update
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
      case None         =>
        throw notFound()
    }
  }

  /**
    * Parses an StructType JSON file contents and converts it to Spark [[StructType]].
    *
    * @param structTypeJson A StructType JSON string.
    * @return The parsed schema as an instance of [[StructType]].
    */
  private def parseStructType(structTypeJson: String): StructType = {
    try {
      sparkMenasConvertor.convertAnyToStructType(structTypeJson)
    } catch {
      case e: IllegalStateException =>
        throw SchemaParsingException(SchemaTypeStruct, e.getMessage, cause = e)
    }
  }

  /**
    * Parses a COBOL copybook file contents and converts it to Spark [[StructType]].
    *
    * @param copybookContents A COBOL copybook contents.
    * @return The parsed schema as an instance of [[StructType]].
    */
  private def parseCopybook(copybookContents: String): StructType = {
    try {
      val parsedSchema = CopybookParser.parseTree(copybookContents)
      val cobolSchema = new CobolSchema(parsedSchema, SchemaRetentionPolicy.CollapseRoot, false)
      cobolSchema.getSparkSchema
    } catch {
      case e: SyntaxErrorException =>
        throw SchemaParsingException(SchemaTypeCopybook, e.getMessage, Some(e.lineNumber), None, Some(e.field), e)
      case e: IllegalStateException =>
        // Cobrix can throw this exception if an unknown AST object is encountered.
        // This might be considered a parsing error.
        throw SchemaParsingException(SchemaTypeCopybook, e.getMessage, cause = e)
    }
  }

}
