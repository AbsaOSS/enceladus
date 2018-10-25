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

var Functions = new function() {
	this.urlBase = ""


	this.ajax = function(sPath, sMethod, oData, fnSuccess, fnError, oControl) {
		if(oControl) oControl.setBusy(true);
		
		var oFormattedData = null;
		if((sMethod.toLowerCase() === "post" || sMethod.toLowerCase() === "put") && typeof(oData) === "object") {
			oFormattedData = JSON.stringify(oData)
		} else oFormattedData = oData;
		
		$.ajax(this.urlBase + sPath, {
			beforeSend : function(oJqXHR, oSettings) {
				var token = $("meta[name='_csrf']").attr("content");
				var header = $("meta[name='_csrf_header']").attr("content");
				console.log("CRSF: " + header + " -> " + token)
				oJqXHR.setRequestHeader(header, token);
			},
			complete: function() {
				if(oControl) oControl.setBusy(false)
			},
			data: oFormattedData,
			dataType : "json",
			contentType: "application/json",
			method : sMethod,
			success : fnSuccess,
			error : fnError
		})
	}
}();