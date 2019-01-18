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

package za.co.absa.enceladus.rest.repositories

import org.mongodb.scala.MongoDatabase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model.Dataset

import scala.reflect.ClassTag
import za.co.absa.enceladus.model.menas.MenasReference
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Projections._
import scala.concurrent.Future

@Repository
class DatasetMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends VersionedMongoRepository[Dataset](mongoDb)(ClassTag(classOf[Dataset])) {

  override private[rest] def collectionName = "dataset"

  
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
      .projection(fields(include("name", "version"), computed("collection", collectionName)))
      .toFuture()
  }
}
