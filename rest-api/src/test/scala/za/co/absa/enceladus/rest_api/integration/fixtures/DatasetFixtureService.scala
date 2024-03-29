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

package za.co.absa.enceladus.rest_api.integration.fixtures

import org.mongodb.scala.MongoDatabase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import za.co.absa.enceladus.rest_api.repositories.DatasetMongoRepository
import za.co.absa.enceladus.model.Dataset

@Component
class DatasetFixtureService @Autowired()(mongoDb: MongoDatabase)
  extends FixtureService[Dataset](mongoDb, DatasetMongoRepository.collectionName)
