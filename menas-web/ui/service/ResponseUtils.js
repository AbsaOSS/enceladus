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

class ResponseUtils {
  /*
   * Parses a response that looks like below and extracts the 'message' field.
   * The method always returns a string and is tolerant to raw response format issues.
   *
   * {
   *   "timestamp": "2019-09-02T11:54:45.574Z",
   *   "id": "67a8b19b-f88c-4792-ba2a-2d67ec1a74b9",
   *   "message": "Detailed error message",
   * }
   *
   */
  static getBadRequestErrorMessage(rawResponse) {
    let errorMessageDetails = "";
    try {
      const oParsedResponse = JSON.parse(rawResponse);
      if (oParsedResponse.message) {
        errorMessageDetails = oParsedResponse.message;
      }
      if (oParsedResponse.error) {
        if (oParsedResponse.error.line) {
          errorMessageDetails += "\nLine: " + oParsedResponse.error.line
        }
        if (oParsedResponse.error.column) {
          errorMessageDetails += "\nColumn: " + oParsedResponse.error.column
        }
        if (oParsedResponse.error.field) {
          errorMessageDetails += "\nField: " + oParsedResponse.error.field
        }
      }
    } catch (e) {
      console.error(`Unable to parse the raw response from the server. Error: ${e}. Response: ${rawResponse}`)
    }
    return errorMessageDetails;
  }

}
