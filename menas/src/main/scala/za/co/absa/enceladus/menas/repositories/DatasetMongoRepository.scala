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

package za.co.absa.enceladus.menas.repositories

import org.mongodb.scala.{Completed, MongoDatabase}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model.{Dataset, MappingTable, Schema}

import scala.reflect.ClassTag
import za.co.absa.enceladus.model.menas.MenasReference
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Projections._
import za.co.absa.enceladus.model

import scala.concurrent.Future

object DatasetMongoRepository {
  val collectionBaseName: String = "dataset"
  val collectionName: String = s"$collectionBaseName${model.CollectionSuffix}"
}

@Repository
class DatasetMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends VersionedMongoRepository[Dataset](mongoDb)(ClassTag(classOf[Dataset])) {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val schemaCollectionName = SchemaMongoRepository.collectionBaseName + model.CollectionSuffix
  private val mappingTableCollectionName = MappingTableMongoRepository.collectionBaseName + model.CollectionSuffix

  private val schemaCollection = mongoDb.getCollection[Schema](schemaCollectionName)
  private val mappingTableCollection = mongoDb.getCollection[MappingTable](mappingTableCollectionName)

  private[menas] override def collectionBaseName: String = DatasetMongoRepository.collectionBaseName

  def getConnectedSchema(name: String, version: Int): Future[Option[Schema]] = {
    schemaCollection.find(getNameVersionFilter(name, Some(version))).headOption()
  }

  def getConnectedMappingTable(name: String, version: Int): Future[Option[MappingTable]] = {
    mappingTableCollection.find(getNameVersionFilter(name, Some(version))).headOption()
  }

  override def getVersion(name: String, version: Int): Future[Option[Dataset]] = {
    super.getVersion(name, version).map(_.map(_.decode))
  }

  override def getAllVersions(name: String, inclDisabled: Boolean = false): Future[Seq[Dataset]] = {
    super.getAllVersions(name, inclDisabled).map(_.map(_.decode))
  }

  override def create(item: Dataset, username: String): Future[Completed] = {
    super.create(item.encode, username)
  }

  override def update(username: String, updated: Dataset): Future[Dataset] = {
    super.update(username, updated.encode).map(_.decode)
  }

  /** This functions allows for searching Datasets, which have certain mapping rules.
   *
   * @param refColVal a number of String, Any pairs, where String is column name, Any is a value. The given column will be compared with the specified value.
   * @return List of Menas references to Datasets, which contain the relevant conformance rules
   */
  def containsMappingRuleRefEqual(refColVal: (String, Any)*): Future[Seq[MenasReference]] = {

    val equals = Filters.and(refColVal.map(col => Filters.eq(col._1, col._2)) :_*)
    val filter = Filters.elemMatch("conformance", equals)

    collection
      .find[MenasReference](filter)
      .projection(fields(include("name", "version"), computed("collection", collectionBaseName)))
      .toFuture()
  }

  /** Find dataset by coordinatorId
   *  @param coordId Coordinator ID
   */
  def findByCoordId(coordId: String): Future[Seq[Dataset]] = {
    val filter = Filters.eq("schedule.activeInstance.coordinatorId", coordId)
    collection
      .find[Dataset](filter)
      .toFuture()
  }

}
