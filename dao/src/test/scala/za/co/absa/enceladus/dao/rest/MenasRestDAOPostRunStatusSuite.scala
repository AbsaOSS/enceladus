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

package za.co.absa.enceladus.dao.rest

import za.co.absa.atum.model.RunStatus
import za.co.absa.enceladus.model.test.factories.RunFactory

object MenasRestDAOPostRunStatusSuite {
  private val uniqueId = "dummyUniqueId"
  private val runStatus = RunFactory.getDummyRunStatus()
  private val url = s"${MenasRestDAOBaseSuite.apiBaseUrl}/runs/updateRunStatus/$uniqueId"
}

import za.co.absa.enceladus.dao.rest.MenasRestDAOPostRunStatusSuite._

class MenasRestDAOPostRunStatusSuite extends MenasRestDAOPostEntitySuite[RunStatus](
  methodName = "updateRunStatus",
  url = url,
  requestBody = runStatus
) {

  override def callMethod(): Boolean = {
    restDAO.updateRunStatus(uniqueId, runStatus)
  }

}
