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

class SchemaManager {

  static getTransitiveSchemas(schemas, rules) {
    rules.map(RuleFactory.createRule).forEach((rule, index) => {
      const schema = $.extend(true, [], schemas[index]);
      rule.apply(schema.fields);
      schemas.push(schema);
    })
  }

  static validateTransitiveSchemas(firstSchema, rules) {
    const schemas = [firstSchema];
    let result = {isValid: true};
    rules.map(RuleFactory.createRule).forEach((rule, index) => {
      const schema = $.extend(true, [], schemas[index]);
      rule.validate(schema.fields, schemas);
      schemas.push(schema);
    });
    return result;
  }

  static validateNameClashes(columnName, schemas, index) {
    const previousSchemaResult = SchemaManager.validateNameClashInPreviousSchemas(columnName, schemas, index);
    if (previousSchemaResult.isValid) {
      return SchemaManager.validateNameClashInFollowingSchemas(columnName, schemas, index);
    } else {
      return previousSchemaResult
    }
  }

  static validateNameClashInPreviousSchemas(columnName, schemas, index) {
    const searchResult = SchemaManager.findColumn(columnName, schemas[index].fields);
    if (searchResult.isFound) {
      let indexOfIntroduction = this.findLatestIndexOfColumnIntroduction(index, columnName, schemas);
      return {isValid: false, index: indexOfIntroduction};
    } else {
      return {isValid: true};
    }
  }

  static findLatestIndexOfColumnIntroduction(index, columnName, schemas) {
    let indexOfIntroduction = 0;
    for (let i = index; i > 0; i--) {
      const searchResult = SchemaManager.findColumn(columnName, schemas[i].fields);
      if (searchResult.isFound) {
        indexOfIntroduction = i;
      } else {
        break;
      }
    }
    return indexOfIntroduction;
  }

  static validateNameClashInFollowingSchemas(columnName, schemas, index) {
    for (let i = index; i < schemas.length; i++) {
      const searchResult = SchemaManager.findColumn(columnName, schemas[i].fields);
      if (searchResult.isFound) {
        return {isValid: false, index: i};
      }
    }
    return {isValid: true};
  }

  static findColumn(columnName, fields) {
    const splitPath = columnName.split(".");

    return splitPath.reduce((fields, path, index) => {
      const field = fields.find(field => field.name === path);
      if (field === undefined) {
        return { isFound: false }
      }
      const children = field.children;
      return (children && children.length > 0 && splitPath.length > index + 1) ? children : { isFound: true, value: field };
    }, fields);
  }

  static validateColumnRemoval(rule, schemas, rules) {
    const columnInPreviousSchema = SchemaManager.findColumn(rule.outputColumn, schemas[rule.order].fields);
    if (!columnInPreviousSchema.isFound) {
      return {isValid: false};
    }

    try {
      const newRules = RuleUtils.insertRule(rules, rule);
      SchemaManager.validateTransitiveSchemas(schemas[0], newRules);
      return {isValid: true};
    } catch (e) {
      return {isValid: false, index: e.order};
    }
  }

}

class RuleFactory {
  static createRule(rule) {
    switch (rule._t) {
      case "CastingConformanceRule":
        return new CastingConformanceRule(rule);
      case "ConcatenationConformanceRule":
        return new ConcatenationConformanceRule(rule);
      case "DropConformanceRule":
        return new DropConformanceRule(rule);
      case "LiteralConformanceRule":
        return new LiteralConformanceRule(rule);
      case "MappingConformanceRule":
        return new MappingConformanceRule(rule);
      case "NegationConformanceRule":
        return new NegationConformanceRule(rule);
      case "SingleColumnConformanceRule":
        return new SingleColumnConformanceRule(rule);
      case "SparkSessionConfConformanceRule":
        return new SparkSessionConfConformanceRule(rule);
      case "UppercaseConformanceRule":
        return new UppercaseConformanceRule(rule);
      default:
        throw new TypeError("Unknown conformance rule type: " + rule._t)
    }
  }
}

class UnknownFiledError extends Error {

  constructor(fieldPath, order) {
    super(`Unable to find field: ${fieldPath}`);
    Error.captureStackTrace(this, UnknownFiledError);
    this._fieldPath = fieldPath;
    this._order = order;
  }

  get fieldPath() {
    return this._fieldPath;
  }

  get order() {
    return this._order;
  }

}
