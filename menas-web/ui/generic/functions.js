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

var Functions = new function () {
  this.csrfHeader = "X-CSRF-TOKEN";
  this.jwtHeader = "JWT"

  this.ajax = function (sPath, sMethod, oData, fnSuccess, fnError, oControl) {
    if (oControl) oControl.setBusy(true);

    let oFormattedData = null;
    if ((sMethod.toLowerCase() === "post" || sMethod.toLowerCase() === "put")
      && (typeof oData === "object")) {
      oFormattedData = JSON.stringify(oData)
    } else {
      oFormattedData = oData;
    }

    return $.ajax(window.apiUrl + sPath, {
      beforeSend: (oJqXHR, oSettings) => {
          let csrfToken = localStorage.getItem("csrfToken");
          let jwtToken = localStorage.getItem("jwtToken");
          console.log("CSRF: " + this.csrfHeader + " -> " + csrfToken);
          oJqXHR.setRequestHeader(this.csrfHeader, csrfToken);
          oJqXHR.setRequestHeader(this.jwtHeader, jwtToken);
      },
      complete: function () {
        if (oControl) oControl.setBusy(false)
      },
      data: oFormattedData,
      dataType: "json",
      contentType: "application/json",
      method: sMethod,
      success: fnSuccess,
      error: (xhr) => {
        if (xhr.status === 401) {
          GenericService.clearSession("Session has expired")
        } else {
          fnError(xhr)
        }
      }
    })
  }
}();
