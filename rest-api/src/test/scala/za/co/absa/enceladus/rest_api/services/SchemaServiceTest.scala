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
package za.co.absa.enceladus.rest_api.services

import za.co.absa.enceladus.model.Schema
import za.co.absa.enceladus.rest_api.repositories.{DatasetMongoRepository, MappingTableMongoRepository, SchemaMongoRepository}
import za.co.absa.enceladus.rest_api.utils.converters.SparkMenasSchemaConvertor
import za.co.absa.enceladus.model.menas.MenasReference
import za.co.absa.enceladus.model.SchemaField
import scala.concurrent.Future
import org.mockito.Mockito
import scala.concurrent.Await
import za.co.absa.enceladus.model.menas.audit._

class SchemaServiceTest extends VersionedModelServiceTest[Schema] {

  val datasetMongoRepository = mock[DatasetMongoRepository]
  val mappingTableMongoRepository = mock[MappingTableMongoRepository]
  val sparkMenasConvertor = mock[SparkMenasSchemaConvertor]
  override val modelRepository = mock[SchemaMongoRepository]
  override val service = new SchemaService(modelRepository, mappingTableMongoRepository, datasetMongoRepository, sparkMenasConvertor)

}
