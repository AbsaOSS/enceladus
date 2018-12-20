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

import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.pojo.PojoCodecProvider
import org.mongodb.scala.MongoDatabase
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import za.co.absa.enceladus.model.Dataset
import za.co.absa.enceladus.model.conformanceRule._

import scala.reflect.ClassTag

@Repository
class DatasetMongoRepository @Autowired()(mongoDb: MongoDatabase)
  extends VersionedMongoRepository[Dataset](mongoDb)(ClassTag(classOf[Dataset])) {

//  override val collection = mongoDb.getCollection[Dataset](collectionName).withCodecRegistry(
//    CodecRegistries.fromProviders(
//      PojoCodecProvider.builder().register(
//        classOf[CastingConformanceRule],
//        classOf[ConcatenationConformanceRule],
//        classOf[DropConformanceRule],
//        classOf[LiteralConformanceRule],
//        classOf[MappingConformanceRule],
//        classOf[NegationConformanceRule],
//        classOf[SingleColumnConformanceRule],
//        classOf[SparkSessionConfConformanceRule],
//        classOf[UppercaseConformanceRule]
//      ).build()
//    )
//  )

  override private[repositories] def collectionName = "dataset"

}
