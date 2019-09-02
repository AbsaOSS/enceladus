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
import za.co.absa.cobrix.spark.cobol.schema.{CobolSchema, SchemaRetentionPolicy}
import za.co.absa.enceladus.menas.repositories.RefCollection
import za.co.absa.enceladus.menas.services.{AttachmentService, SchemaService}
import za.co.absa.enceladus.menas.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.model.menas._

import scala.util.control.NonFatal

@RestController
@RequestMapping(Array("/api/schema"))
class SchemaController @Autowired() (
  schemaService:     SchemaService,
  attachmentService: AttachmentService,
  sparkMenasConvertor: SparkMenasSchemaConvertor)
  extends VersionedModelController(schemaService) {

  import za.co.absa.enceladus.menas.utils.implicits._

  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(Array("/upload"))
  @ResponseStatus(HttpStatus.CREATED)
  def handleFileUpload(@AuthenticationPrincipal principal: UserDetails,
                       @RequestParam file: MultipartFile,
                       @RequestParam version: Int,
                       @RequestParam name: String,
                       @RequestParam format: Optional[String]): CompletableFuture[_] = {
    val origFormat: Option[String] = format
    val fileContent = new String(file.getBytes)

    val sparkStruct = origFormat match {
      case Some("struct") | Some("") | None =>
        try {
          sparkMenasConvertor.convertAnyToStructType(fileContent)
        } catch {
          case NonFatal(e) => throw badRequest(e.getMessage)
        }
      case Some("copybook") =>
        try {
          val parsedSchema = CopybookParser.parseTree(fileContent)
          val cobolSchema = new CobolSchema(parsedSchema, SchemaRetentionPolicy.CollapseRoot, false)
          cobolSchema.getSparkSchema
        } catch {
          case NonFatal(e) => throw badRequest(e.getMessage)
        }
      case Some(x) =>
        throw badRequest(s"$x is not a recognized schema format. Menas currently supports: 'struct' and 'copybook'")
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

}
