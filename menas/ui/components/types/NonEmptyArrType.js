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

sap.ui.define(['sap/ui/model/SimpleType', 'sap/ui/model/FormatException', 'sap/ui/model/ParseException'],
  function(SimpleType, FormatException, ParseException) {
  "use strict";

  var NonEmptyArrType = SimpleType.extend("components.types.NonEmptyArrType", {
    constructor : function () {
      SimpleType.apply(this, arguments);
      this.sName = "NonEmptyArrType";
    }
  });

  NonEmptyArrType.prototype.formatValue = function(oValue, sInternalType) {
    if(Array.isArray(oValue) && sInternalType === "string") {
      return oValue.join(", ");
    }
    return oValue;
  };

  NonEmptyArrType.prototype.parseValue = function(oValue, sInternalType) {
    return oValue;
  };

  NonEmptyArrType.prototype.validateValue = function(oValue) {
    if(!oValue || (Array.isArray(oValue) && oValue.length === 0)) {
      throw new sap.ui.model.ValidateException("At least one value has to be selected");
    }
  };
  return NonEmptyArrType;

});
