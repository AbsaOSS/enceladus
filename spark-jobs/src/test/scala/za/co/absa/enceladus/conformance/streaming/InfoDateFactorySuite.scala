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

package za.co.absa.enceladus.conformance.streaming

import org.apache.commons.configuration2.Configuration
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import za.co.absa.enceladus.conformance.HyperConformanceAttributes._

class InfoDateFactorySuite extends WordSpec with Matchers with MockitoSugar {
  private val configStub: Configuration = mock[Configuration]

  "InfoDateFactory" should {
    "return an explicit info date factory from config" when {
      "an explicit report date is specified in config" in {
        when(configStub.containsKey(reportDateKey)).thenReturn(true)
        when(configStub.getString(reportDateKey)).thenReturn("2019-01-01")

        val infoDateFactory = InfoDateFactory.getFactoryFromConfig(configStub)

        assert(infoDateFactory.isInstanceOf[InfoDateLiteralFactory])
      }

      "several configuration options are specified so the explicit report date takes precedence" in {
        when(configStub.containsKey(reportDateKey)).thenReturn(true)
        when(configStub.getString(reportDateKey)).thenReturn("2019-01-01")
        when(configStub.containsKey(eventTimestampColumnKey)).thenReturn(true)
        when(configStub.getString(eventTimestampColumnKey)).thenReturn("EV_TIME")

        val infoDateFactory = InfoDateFactory.getFactoryFromConfig(configStub)

        assert(infoDateFactory.isInstanceOf[InfoDateLiteralFactory])
      }
    }

    "return an event time strategy when an event timestamp column is specified in config config" when {
      "an explicit report date is specified in config" in {
        when(configStub.containsKey(reportDateKey)).thenReturn(false)
        when(configStub.containsKey(eventTimestampColumnKey)).thenReturn(true)
        when(configStub.getString(eventTimestampColumnKey)).thenReturn("EV_TIME")

        val infoDateFactory = InfoDateFactory.getFactoryFromConfig(configStub)

        assert(infoDateFactory.isInstanceOf[InfoDateFromColumnFactory])
      }
    }

    "return an processing time strategy by default" in {
      when(configStub.containsKey(reportDateKey)).thenReturn(false)
      when(configStub.containsKey(eventTimestampColumnKey)).thenReturn(false)

      val infoDateFactory = InfoDateFactory.getFactoryFromConfig(configStub)

      assert(infoDateFactory.isInstanceOf[InfoDateFromProcessingTimeFactory])
    }
  }

}
