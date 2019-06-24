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

  static validatePathOfStructs(columnName, fields) {
    const splitPath = columnName.split(".");
    let pathIndex = 0;

    const helper = function (fields, pathSection, accumlatedValidation) {
      const field = fields.find(f => f.name === pathSection);
      if (field === undefined) {
        return accumlatedValidation;
      }

      if (field.type === "struct" || field.elementType === "struct") {
        return helper(field.children, splitPath[++pathIndex], { isValid: true, value: field });
      }

      return { isValid: false, error: `"${splitPath.slice(0, pathIndex + 1).join(".")}" is of type "${field.type}", only "struct" types can be nested` };
    };

    return helper(fields, splitPath[pathIndex], { isValid: true });
  }

  static findColumn(columnName, fields) {
    const splitPath = columnName.split(".");
    let pathIndex = 0;

    const helper = function (fields, pathSection) {
      const field = fields.find(f => f.name === pathSection);
      if (field === undefined) {
        return { isFound: false }
      }

      if (pathIndex === splitPath.length - 1) {
        return { isFound: true, value: field };
      }

      return helper(field.children, splitPath[++pathIndex])
    };

    return helper(fields, splitPath[pathIndex])
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
