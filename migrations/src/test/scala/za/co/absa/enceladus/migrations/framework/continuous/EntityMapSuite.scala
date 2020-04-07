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

package za.co.absa.enceladus.migrations.framework.continuous

import org.scalatest.FunSuite
import za.co.absa.enceladus.migrations.continuous.EntityVersionMap
import za.co.absa.enceladus.migrations.framework.continuous.fixture.EntityVersionMapMock

class EntityMapSuite extends FunSuite {

  test("Test entity version map returns correct mapping when it is available") {
    val enp: EntityVersionMap = new EntityVersionMapMock

    enp.add("dataset", "test", 1, 2)
    val versionAvailable = enp.get("dataset", "test", 1)
    val versionAbsent = enp.get("dataset", "test", 3)

    assert(versionAvailable.isDefined)
    assert(versionAvailable.get == 2)
    assert(versionAbsent.isEmpty)
  }

  test("Test entity version map throws if there is an attempt to overwrite an existing mapping") {
    val enp: EntityVersionMap = new EntityVersionMapMock

    enp.add("dataset", "test", 1, 2)

    // Adding the same mapping is ok
    enp.add("dataset", "test", 1, 2)

    // Overwriting a mapping is not allowed
    assertThrows[IllegalStateException] {
      enp.add("dataset", "test", 1, 3)
    }
  }
}
