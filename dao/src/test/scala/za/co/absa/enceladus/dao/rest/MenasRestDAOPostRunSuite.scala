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

import org.springframework.http.HttpHeaders
import za.co.absa.enceladus.dao.{DaoException, UnauthorizedException}
import za.co.absa.enceladus.model.Run
import za.co.absa.enceladus.model.test.factories.RunFactory

class MenasRestDAOPostRunSuite extends MenasRestDAOBaseSuite {

  val url: String = s"${MenasRestDAOBaseSuite.apiBaseUrl}/runs"
  val uniqueId: String = "503a5b7f-2171-41bd-9a46-91e1135dcb01"
  val requestRun: Run = RunFactory.getDummyRun(uniqueId = None)
  val responseRun: Run = requestRun.copy(uniqueId = Some(uniqueId))
  val responseRunJson: String = """{
                                  |  "uniqueId": "503a5b7f-2171-41bd-9a46-91e1135dcb01",
                                  |  "runId": 1,
                                  |  "dataset": "dummyDataset",
                                  |  "datasetVersion": 1,
                                  |  "splineRef": {
                                  |    "sparkApplicationId": "dummySparkApplicationId",
                                  |    "outputPath": "dummyOutputPath"
                                  |  },
                                  |  "startDateTime": "04-12-2017 16:19:17 +0200",
                                  |  "runStatus": {
                                  |    "status": {
                                  |      "enumClass": "za.co.absa.atum.model.RunState",
                                  |      "value": "allSucceeded"
                                  |    },
                                  |    "error": null
                                  |  },
                                  |  "controlMeasure": {
                                  |    "metadata": {
                                  |      "sourceApplication": "dummySourceApplication",
                                  |      "country": "dummyCountry",
                                  |      "historyType": "dummyHistoryType",
                                  |      "dataFilename": "dummyDataFilename",
                                  |      "sourceType": "dummySourceType",
                                  |      "version": 1,
                                  |      "informationDate": "04-12-2017 16:19:17 +0200",
                                  |      "additionalInfo": {}
                                  |    },
                                  |    "runUniqueId": "078ece48-9180-44f2-bf24-18a036ab018d",
                                  |    "checkpoints": []
                                  |  }
                                  |}""".stripMargin

  def callMethod(): String = {
    restDAO.storeNewRunObject(requestRun)
  }

  "MenasRestDAO::storeNewRunObject" should {
    "return true on 201 CREATED" in {
      stubCreatedPostRequest(url, new HttpHeaders(), requestRun, responseRunJson)

      val result = callMethod()
      result should be(uniqueId)
    }

    "return 201 CREATED after successful retry on 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedPostRequest(url, expiredSessionHeaders, requestRun)
      val renewedSessionHeaders = stubAuthSuccess()
      stubCreatedPostRequest(url, renewedSessionHeaders, requestRun, responseRunJson)

      val result = callMethod()
      result should be(uniqueId)
    }

    "throw an error on unsuccessful retry after 401 Unauthorized" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubUnauthorizedPostRequest(url, expiredSessionHeaders, requestRun)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        callMethod()
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "return 201 CREATED after successful retry on 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenPostRequest(url, expiredSessionHeaders, requestRun)
      val renewedSessionHeaders = stubAuthSuccess()
      stubCreatedPostRequest(url, renewedSessionHeaders, requestRun, responseRunJson)

      val result = callMethod()
      result should be(uniqueId)
    }

    "throw an error on unsuccessful retry after 403 Forbidden" in {
      val expiredSessionHeaders = stubExpiredSession()
      stubForbiddenPostRequest(url, expiredSessionHeaders, requestRun)
      stubAuthFailure()

      val exception = intercept[UnauthorizedException] {
        callMethod()
      }
      exception shouldBe UnauthorizedException("Authentication failure")
    }

    "throw an error on 404 Not Found" in {
      stubNotFoundPostRequest(url, requestRun)

      val exception = intercept[DaoException] {
        callMethod()
      }
      exception shouldBe DaoException("Entity not found - 404")
    }

    "throw an error on 500 Internal Server Error" in {
      stubInternalServerErrorPostRequest(url, requestRun)

      val exception = intercept[DaoException] {
        callMethod()
      }
      exception shouldBe DaoException("Response - 500 : None")
    }
  }

}
