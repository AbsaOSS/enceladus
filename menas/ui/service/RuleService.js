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

class RuleService {

  static removeRule(oCurrentDataset, iRuleIndex) {
    let conformance = oCurrentDataset["conformance"]
      .filter((_, index) => index !== iRuleIndex)
      .sort((first, second) => first.order > second.order)
      .map((currElement, index) => {
        return {...currElement, order: index}
      });

    return {...oCurrentDataset, conformance: conformance};
  }

  static moveUpRule(originalRules, schemaFields, iRuleIndex) {
    const conformanceRules = $.extend(true, [], originalRules);
    if (iRuleIndex === 0) {
      return new InvalidResult("Unable to move up first rule.");
    }

    RuleService.swap(conformanceRules, iRuleIndex, iRuleIndex - 1);

    const newRules = conformanceRules
      .map((currElement, index) => {
        return {...currElement, order: index}
      });

    return new ValidResult(newRules);
  }

  static moveDownRule(originalRules, schemaFields, iRuleIndex) {
    const conformanceRules = $.extend(true, [], originalRules);
    if (iRuleIndex === conformanceRules.length - 1) {
      return new InvalidResult("Unable to move down last rule.");
    }

    RuleService.swap(conformanceRules, iRuleIndex, iRuleIndex + 1);

    const newRules = conformanceRules
      .map((currElement, index) => {
        return {...currElement, order: index}
      });

    return new ValidResult(newRules);
  }

  static swap(array, index1, index2) {
    [array[index1], array[index2]] = [array[index2], array[index1]];
  }

}

class Result {

  constructor(result, isValid, errorMessage) {
    this._result = result;
    this._isValid = isValid;
    this._errorMessage = errorMessage;
  }

  get result() {
    return this._result;
  }

  get isValid() {
    return this._isValid;
  }

  get errorMessage() {
    return this._errorMessage;
  }

}

class ValidResult extends Result {

  constructor(result) {
    super(result, true);
  }

}

class InvalidResult extends Result {

  constructor(errorMessage) {
    super(null, false, errorMessage);
  }

}
