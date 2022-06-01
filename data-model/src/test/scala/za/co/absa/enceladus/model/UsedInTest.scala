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

package za.co.absa.enceladus.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.enceladus.model.menas.MenasReference

class UsedInTest extends AnyFlatSpec with Matchers {

  private val exampleRef = MenasReference(Some("collection1"), "entity1", 1)

  "UsedIn" should "correctly evaluate .nonEmpty" in {
    UsedIn(Some(Seq(exampleRef)), Some(Seq(exampleRef))).nonEmpty shouldBe true
    UsedIn(Some(Seq(exampleRef)), Some(Seq.empty)).nonEmpty shouldBe true
    UsedIn(Some(Seq(exampleRef)), None).nonEmpty shouldBe true

    UsedIn(Some(Seq.empty), Some(Seq(exampleRef))).nonEmpty shouldBe true
    UsedIn(None, Some(Seq(exampleRef))).nonEmpty shouldBe true

    UsedIn(Some(Seq.empty), Some(Seq.empty)).nonEmpty shouldBe false
    UsedIn(None, Some(Seq.empty)).nonEmpty shouldBe false
    UsedIn(Some(Seq.empty), None).nonEmpty shouldBe false
    UsedIn(None, None).nonEmpty shouldBe false
  }

  it should "correctly evaluate .empty" in {
    UsedIn(Some(Seq(exampleRef)), Some(Seq(exampleRef))).isEmpty shouldBe false
    UsedIn(Some(Seq(exampleRef)), Some(Seq.empty)).isEmpty shouldBe false
    UsedIn(Some(Seq(exampleRef)), None).isEmpty shouldBe false

    UsedIn(Some(Seq.empty), Some(Seq(exampleRef))).isEmpty shouldBe false
    UsedIn(None, Some(Seq(exampleRef))).isEmpty shouldBe false

    UsedIn(Some(Seq.empty), Some(Seq.empty)).isEmpty shouldBe true
    UsedIn(None, Some(Seq.empty)).isEmpty shouldBe true
    UsedIn(Some(Seq.empty), None).isEmpty shouldBe true
    UsedIn(None, None).isEmpty shouldBe true
  }

  it should "normalize" in {
    UsedIn(Some(Seq(exampleRef)), Some(Seq(exampleRef))).normalized shouldBe UsedIn(Some(Seq(exampleRef)), Some(Seq(exampleRef)))

    UsedIn(Some(Seq(exampleRef)), Some(Seq.empty)).normalized shouldBe UsedIn(Some(Seq(exampleRef)), None)
    UsedIn(Some(Seq(exampleRef)), None).normalized shouldBe UsedIn(Some(Seq(exampleRef)), None)

    UsedIn(Some(Seq.empty), Some(Seq(exampleRef))).normalized shouldBe UsedIn(None, Some(Seq(exampleRef)))
    UsedIn(None, Some(Seq(exampleRef))).normalized shouldBe UsedIn(None, Some(Seq(exampleRef)))

    UsedIn(Some(Seq.empty), Some(Seq.empty)).normalized shouldBe UsedIn(None, None)
    UsedIn(None, None).normalized shouldBe UsedIn(None, None)




  }



}
