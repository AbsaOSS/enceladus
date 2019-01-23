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
package za.co.absa.enceladus.rest.services

import za.co.absa.enceladus.model.MappingTable
import za.co.absa.enceladus.rest.repositories.{ DatasetMongoRepository, MappingTableMongoRepository }

class MappingTableServiceTest extends VersionedModelServiceTest[MappingTable] {

  val datasetService = mock[DatasetService]
  val auditTrailService = mock[AuditTrailService]
  override val modelRepository = mock[MappingTableMongoRepository]
  override val service = new MappingTableService(modelRepository, auditTrailService, datasetService)

}
