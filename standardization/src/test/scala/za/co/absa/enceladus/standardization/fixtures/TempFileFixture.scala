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

package za.co.absa.enceladus.standardization.fixtures

import java.io.{DataOutputStream, File, FileOutputStream}
import java.nio.charset.{Charset, StandardCharsets}

/**
  * This fixture adds ability for a unit test to create temporary files for using them in the tests.
  */
trait TempFileFixture {
  /**
    * Creates a temporary text file and returns the full path to it
    *
    * @param prefix  The prefix string to be used in generating the file's name;
    *                must be at least three characters long
    * @param suffix  The suffix string to be used in generating the file's name;
    *                may be <code>null</code>, in which case the suffix <code>".tmp"</code> will be used
    * @param charset A charset of the data in the temporaty text file
    * @param content A contents to put to the file
    * @return The full path to the temporary file
    */
  def createTempFile(prefix: String, suffix: String, charset: Charset, content: String): File = {
    val tempFile = File.createTempFile(prefix, suffix)
    val ostream = new DataOutputStream(new FileOutputStream(tempFile))
    ostream.write(content.getBytes(charset))
    ostream.close()
    tempFile
  }

}
