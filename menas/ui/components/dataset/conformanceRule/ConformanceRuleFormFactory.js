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

class ConformanceRuleFormFragmentFactory {

  constructor(dialog) {
    this._dialog = dialog;
    this._formFragments = {};
  }

  get dialog() {
    return this._dialog;
  }

  get formFragments() {
    return this._formFragments;
  }

  getFormFragment(sFragmentName) {
    let oFormFragment = this.formFragments[sFragmentName];
    if (oFormFragment) {
      return oFormFragment;
    }

    oFormFragment = sap.ui.xmlfragment(sFragmentName, "components.dataset.conformanceRule." + sFragmentName + ".add", this.dialog);
    this.formFragments[sFragmentName] = oFormFragment;
    return this.formFragments[sFragmentName];
  }

}

class ConformanceRuleFormRepository {

  constructor() {
    this._formsMap = {
      "CastingConformanceRule": new CastingConformanceRuleForm(),
      "ConcatenationConformanceRule": new ConcatenationConformanceRuleForm(),
      "DropConformanceRule": new DropConformanceRuleForm(),
      "LiteralConformanceRule": new LiteralConformanceRuleForm(),
      "MappingConformanceRule": new MappingConformanceRuleForm(),
      "NegationConformanceRule": new NegationConformanceRuleForm(),
      "SingleColumnConformanceRule": new SingleColumnConformanceRuleForm(),
      "SparkSessionConfConformanceRule": new SparkSessionConfConformanceRuleForm(),
      "UppercaseConformanceRule": new UppercaseConformanceRuleForm()
    };

    this._all = Object.values(this.formsMap);
  }

  get formsMap() {
    return this._formsMap;
  }

  get all() {
    return this._all;
  }

  byType(ruleType) {
    return this.formsMap[ruleType]
  }

}
