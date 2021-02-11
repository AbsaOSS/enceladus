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

jQuery.sap.require("sap.ui.core.message.Message")
jQuery.sap.require("sap.ui.core.MessageType")
jQuery.sap.require("sap.ui.core.ValueState")

/**
 * This is a generic UI5 Validator class. The original idea was based on https://github.com/qualiture/ui5-validator.
 * This version is written using ES6 features and in a more functional way.
 */
sap.ui.define(["sap/ui/core/message/Message", "sap/ui/core/MessageType", "sap/ui/core/ValueState"], function(Message, MessageType, ValueState) {

  const Validator = function() {}

  Validator.prototype._aPossibleAggregations = ["items", "content", "form", "formContainers", "formElements", "fields", "sections", "subSections", "_grid", "cells", "_page"];
  Validator.prototype._aValidateProperties = ["value", "selectedKey", "selectedKeys", "text"];

  Validator.prototype._andReduceOp = function(a, b) {
    return a && b;
  }

  Validator.prototype._traverseControlAggregations = function(oControl, fnTransform, fnReduceOp) {
    let fnReduce = fnReduceOp ? fnReduceOp : this._andReduceOp;

    if(!oControl || !oControl.getVisible || !oControl.getVisible()) return true;

    const aAllAggregations = this._aPossibleAggregations.map(currAgg => {
      const aAggContent = oControl.getAggregation(currAgg);

      const currRes = fnTransform(oControl);

      let aggRes;

      if(Array.isArray(aAggContent)) {
        const aRes = aAggContent.map(oCtl => this._traverseControlAggregations(oCtl, fnTransform, fnReduceOp));
        const aReduced = _.reduce(aRes, fnReduceOp, true);
        aggRes = aReduced;
      } else if(aAggContent) {
        aggRes = this._traverseControlAggregations(aAggContent, fnTransform, fnReduceOp);
      } else {
        aggRes = true;
      }

      return fnReduce(currRes, aggRes);
    })
    return _.reduce(aAllAggregations, fnReduce, true);
  }

  Validator.prototype.clearAll = function(oControl) {
    sap.ui.getCore().getMessageManager().removeAllMessages();
    this._clearAll(oControl);
  }

  Validator.prototype._clearAll = function(oControl) {
    this._traverseControlAggregations(oControl, oCurrCtl => {
      if(oCurrCtl.setValueState) {
        oCurrCtl.setValueState(ValueState.None);
      }
    })
  }

  Validator.prototype._validateProperty = function(oControl, sProperty) {
    const oBinding = oControl.getBinding(sProperty);
    if(!oBinding || !oBinding.getType()) return true;
    try {
      const oType = oBinding.getType();
      const oExternalValue = oControl.getProperty(sProperty);
      const oInternalValue = oType.parseValue(oExternalValue, oBinding.sInternalType);
      oType.validateValue(oInternalValue);
      return true;
    } catch(ex) {
      sap.ui.getCore().getMessageManager().addMessages(new Message({
        message: ex.message,
        type: MessageType.Error,
        target: (oBinding.getContext() ? oBinding.getContext().getPath() + "/" : "") + oBinding.getPath(),
        processor: oBinding.getModel()
      }));
      return false;
    }
  }

  Validator.prototype.validate = function(oControl) {
    sap.ui.getCore().getMessageManager().removeAllMessages();
    return this._traverseControlAggregations(oControl, this._validate.bind(this), this._andReduceOp);
  }

  Validator.prototype._validate = function(oControl) {
    if((oControl instanceof sap.ui.core.Control ||
        oControl instanceof sap.ui.layout.form.FormContainer ||
        oControl instanceof sap.ui.layout.form.FormElement) && oControl.getVisible()) {
      const aProperties = this._aValidateProperties.map(sProperty => this._validateProperty(oControl, sProperty));

      return _.reduce(aProperties, this._andReduceOp, true);
    } else {
      return true;
    }
  }

  return Validator;
})
