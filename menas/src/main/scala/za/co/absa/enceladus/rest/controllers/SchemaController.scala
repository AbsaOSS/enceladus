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

package za.co.absa.enceladus.rest.controllers

import java.util.concurrent.CompletableFuture

import org.apache.spark.sql.types.{ DataType, StructType }
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.web.bind.annotation._
import org.springframework.web.multipart.MultipartFile
import za.co.absa.enceladus.model.menas._
import za.co.absa.enceladus.rest.repositories.RefCollection
import za.co.absa.enceladus.rest.services.{ AttachmentService, SchemaService }
import za.co.absa.enceladus.rest.utils.converters.SparkMenasSchemaConvertor

@RestController
@RequestMapping(Array("/api/schema"))
class SchemaController @Autowired() (
  schemaService:     SchemaService,
  attachmentService: AttachmentService, sparkMenasConvertor: SparkMenasSchemaConvertor)
  extends VersionedModelController(schemaService) {

  import za.co.absa.enceladus.rest.utils.implicits._
  import scala.concurrent.ExecutionContext.Implicits.global

  @PostMapping(path = Array("/upload"))
  def handleFileUpload(@AuthenticationPrincipal principal: UserDetails, @RequestParam("file") file: MultipartFile,
                       @RequestParam("version") version: Int, @RequestParam("name") name: String): CompletableFuture[_] = {
    val origFile = MenasAttachment(refCollection = RefCollection.SCHEMA.name().toLowerCase, refName = name, refVersion = version + 1, attachmentType = MenasAttachment.ORIGINAL_SCHEMA_ATTACHMENT,
      filename = file.getOriginalFilename, fileContent = file.getBytes, fileMIMEType = file.getContentType)

    val struct = DataType.fromJson(new String(file.getBytes)).asInstanceOf[StructType]

    for {
      upload <- attachmentService.uploadAttachment(origFile)
      update <- schemaService.update(principal.getUsername, name)(oldSchema => oldSchema.copy(fields = sparkMenasConvertor.convertSparkToMenasFields(struct.fields).toList))
    } yield update
  }
}
