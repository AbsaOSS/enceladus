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

jQuery.sap.require("sap.ui.core.format.DateFormat");
jQuery.sap.require("sap.ui.core.Locale");

var Formatters = new function() {

  let defaultDateFormat = sap.ui.core.format.DateFormat.getDateTimeInstance({
    style: "short"
  }, new sap.ui.core.Locale("en_GB"));

  this.dateShortFormatter = function(oDate) {
    if (!oDate)
      return "";
    return defaultDateFormat.format(oDate)
  };

  this.nonNullArrFormatter = function(aArr) {
    if(!aArr) return [];
    return aArr;
  };

  this.stringDateShortFormatter = function(sDate) {
    if (!sDate)
      return "";
    var oDate = new Date(sDate);
    return defaultDateFormat.format(oDate)
  };

  this.not = function(bSth) {
    return !bSth;
  };

  this.nonEmptyObject = function(oObj) {
    return (oObj !== null) && (typeof (oObj) !== "undefined") && (Object.keys(oObj).length !== 0)
  };

  this.nonEmptyAndNonNullFilled = function(oObj) {
    return Formatters.nonEmptyObject(oObj) && oObj.filter(x => x).length !== 0 // [null] will return false, too
  };

  this.isDefinedAndTrue = function(oObj) {
    return (oObj !== null) && (typeof (oObj) !== "undefined") && oObj == true
  };

  this.objToKVArray = function(oObj) {
    if(oObj === null || typeof(oObj) === "undefined") return []
    else {
      var res = [];
      for(var i in oObj) {
        res.push({
          key: i,
          value: oObj[i]
        })
      }
      return res;
    }
  };

  this.infoDatePattern = "yyyy-MM-dd";

  this.infoDateFormat = sap.ui.core.format.DateFormat.getInstance({
    pattern: this.infoDatePattern,
    calendarType: sap.ui.core.CalendarType.Gregorian
  });

  this.infoDateToString = function (oDate) {
    if (!oDate)
      return "";
    return this.infoDateFormat.format(oDate)
  };

  this.toStringInfoDate = function(oDate) {
    return this.infoDateFormat.format(oDate);
  };

  this.statusToPrettyString = function(sStatus) {
    switch(sStatus) {
      case "failed" :
        return "Failed";
      case "running" :
        return "Running";
      case "stageSucceeded" :
        return "Stage Succeeded";
      case "allSucceeded" :
        return "All Succeeded";
      default:
        return sStatus
    }
  };

  this.italicMissingProp = function (sStatus) {
    if (sStatus === null) {
      this.addStyleClass("missingPropertyValue");
      return 'Missing';
    }
    return sStatus;
  };

}();
