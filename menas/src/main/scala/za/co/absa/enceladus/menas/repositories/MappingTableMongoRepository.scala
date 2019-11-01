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

package za.co.absa.enceladus.menas.repositories

import org.mongodb.scala.MongoDatabase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model
import za.co.absa.enceladus.model.MappingTable

import scala.reflect.ClassTag

object MappingTableMongoRepository {
  val collectionBaseName: String = "mapping_table"
  val collectionName: String = s"$collectionBaseName${model.CollectionSuffix}"
}

@Repository
class MappingTableMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends VersionedMongoRepository[MappingTable](mongoDb)(ClassTag(classOf[MappingTable])) {

  override private[menas] def collectionBaseName = MappingTableMongoRepository.collectionBaseName

}
