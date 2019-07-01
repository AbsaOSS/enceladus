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

package za.co.absa.enceladus.menas.factories

import za.co.absa.enceladus.model.menas.MenasAttachment

object AttachmentFactory {

  def getDummyAttachment(refCollection: String = "dummyRefCollection",
                         refName: String = "dummyName",
                         refVersion: Int = 1,
                         attachmentType: String = "original_schema",
                         filename: String = "dummySchema.json",
                         fileContent: Array[Byte] = Array(),
                         fileMIMEType: String = "application/json"): MenasAttachment = {
    MenasAttachment(
      refCollection,
      refName,
      refVersion,
      attachmentType,
      filename,
      fileContent,
      fileMIMEType)
  }

}
