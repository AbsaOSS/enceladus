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

package za.co.absa.enceladus.rest_api.repositories.v3

import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.Filters
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunMongoRepositoryV3Test extends AnyFlatSpec with Matchers {
  import RunMongoRepositoryV3._

  behavior of "RunMongoRepositoryV3"

  it should "combineFilters" in {
    RunMongoRepositoryV3.combineFilters(BsonDocument(), BsonDocument()) shouldBe emptyBsonFilter
    RunMongoRepositoryV3.combineFilters(Filters.eq("a", 1), BsonDocument()) shouldBe Filters.eq("a", 1)
    RunMongoRepositoryV3.combineFilters(BsonDocument(), Filters.eq("b", 0)) shouldBe Filters.eq("b", 0)
    RunMongoRepositoryV3.combineFilters(Filters.eq("a", 3), Filters.gte("c", -1)) shouldBe
      Filters.and(Filters.eq("a", 3), Filters.gte("c", -1))
  }

}
