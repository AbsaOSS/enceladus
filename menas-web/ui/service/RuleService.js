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

class RuleService {

  static UP = -1;
  static DOWN = 1;

  static removeRule(originalRules, schema, ruleIndex) {
    const conformanceRules = $.extend(true, [], originalRules);

    const newRules = conformanceRules
      .filter((_, index) => index !== ruleIndex)
      .sort((first, second) => first.order > second.order)
      .map(this.orderByIndex);

    try {
      SchemaManager.validateTransitiveSchemas(schema, newRules);
    } catch (e) {
      const indexOfBrokenRule = e.order + 1; // adding 1 to account for the order being on a schema without the removed rule
      return new InvalidResult(`Unable to remove conformance rule ${ruleIndex + 1} as it would break the dependency on column "${e.fieldPath}" by rule ${indexOfBrokenRule + 1}`)
    }

    return new ValidResult(newRules);
  }

  static orderByIndex(currElement, index) {
    return {...currElement, order: index}
  }

  static moveRuleUp(rules, schema, ruleIndex) {
    if (ruleIndex === 0) {
      return new InvalidResult("Unable to move up first rule.");
    }

    return RuleService.moveRule(rules, schema, ruleIndex, RuleService.UP);
  }

  static moveRuleDown(rules, schema, ruleIndex) {
    if (ruleIndex === rules.length - 1) {
      return new InvalidResult("Unable to move down last rule.");
    }

    return RuleService.moveRule(rules, schema, ruleIndex, RuleService.DOWN);
  }

  static moveRule(originalRules, schema, ruleIndex, direction) {
    const conformanceRules = $.extend(true, [], originalRules);

    const otherRuleIndex = ruleIndex + direction;
    RuleService.swap(conformanceRules, ruleIndex, otherRuleIndex);

    const newRules = conformanceRules.map(this.orderByIndex);

    try {
      SchemaManager.validateTransitiveSchemas(schema, newRules);
    } catch (e) {
      const smallerIndex = Math.min(ruleIndex, otherRuleIndex);
      const largerIndex = Math.max(ruleIndex, otherRuleIndex);
      return new InvalidResult(`Unable to move conformance rule as it would break the dependency on column "${e.fieldPath}" between rule ${smallerIndex + 1} and ${largerIndex + 1}`)
    }

    return new ValidResult(newRules);
  }

  static swap(array, index1, index2) {
    [array[index1], array[index2]] = [array[index2], array[index1]];
  }

}

class RuleUtils {

  static insertRule(originalRules, newRule) {
    return RuleUtils.spliceRule(originalRules, newRule, 0)
  }

  static replaceRule(originalRules, newRule) {
    return RuleUtils.spliceRule(originalRules, newRule, 1);
  }

  static spliceRule(originalRules, newRule, deleteCount) {
    const conformanceRules = $.extend(true, [], originalRules);
    conformanceRules.splice(newRule.order, deleteCount, newRule);
    return conformanceRules.map(RuleService.orderByIndex);
  }

}
