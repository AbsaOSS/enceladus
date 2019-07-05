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

package za.co.absa.enceladus.utils.implicits

import java.io.ByteArrayOutputStream
import org.apache.spark.sql.DataFrame

object DataFrameImplicits {
  implicit class DataFrameEnhancements(val df: DataFrame) {

    private def gatherData(showFnc: () => Unit): String = {
      val outCapture = new ByteArrayOutputStream
      Console.withOut(outCapture) {
        showFnc()
      }
      val dfData = new String(outCapture.toByteArray).replace("\r\n", "\n")
      dfData
    }

    def dataAsString(): String = {
      val showFnc: () => Unit = df.show
      gatherData(showFnc)
    }

    def dataAsString(truncate: Boolean): String = {
      val showFnc:  () => Unit = ()=>{df.show(truncate)}
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Boolean): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate)
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Int): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate)
      gatherData(showFnc)
    }

    def dataAsString(numRows: Int, truncate: Int, vertical: Boolean): String = {
      val showFnc: ()=>Unit = () => df.show(numRows, truncate, vertical)
      gatherData(showFnc)
    }

  }

}
