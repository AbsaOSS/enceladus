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

sap.ui.define(['components/types/NonEmptyArrType', 'sap/ui/model/FormatException', 'sap/ui/model/ParseException'],
  function(NonEmptyArrType, FormatException, ParseException) {
  "use strict";

  var CronEntryType = NonEmptyArrType.extend("components.types.CronEntryType", {
    constructor : function () {
      NonEmptyArrType.apply(this, arguments);
      this.sName = "CronEntryType";
    }
  });

  CronEntryType.prototype.formatValue = function(oValue, sInternalType) {
    return NonEmptyArrType.prototype.formatValue(oValue, sInternalType);
  };

  CronEntryType.prototype.parseValue = function(oValue, sInternalType) {
    return NonEmptyArrType.prototype.parseValue(oValue, sInternalType);
  };

  CronEntryType.prototype.validateValue = function(oValue) {
    NonEmptyArrType.prototype.validateValue(oValue);
    const bHasAny = oValue.find((el) => {return el === "*"})
    if(bHasAny && oValue.length > 1) {
      throw new sap.ui.model.ValidateException("Either select 'Any' or 1..n specific entries, but not both!");
    }
  };
  return CronEntryType;

});
