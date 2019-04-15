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

  static updateTransitiveSchema(schemaFields, rules) {
    return rules.map(RuleFactory.createRule).reduce((fields, rule) => {
      return rule.apply(fields);
    }, schemaFields);
  };

}


// TODO: break this down into files (#224) after we bundle the resources (#314)
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

class ConformanceRule {

  constructor(rule) {
    if (this.apply === undefined) {
      throw new TypeError("Abstract function 'apply' not implemented.");
    }

    this._rule = rule;

    const outputCol = rule.outputColumn;
    const index = outputCol.lastIndexOf(".");
    const name = (index === -1) ? outputCol : outputCol.slice(index + 1);
    const path = (index === -1) ? "" : outputCol.slice(0, index);
    this._outputCol = {name: name, path: path};
  }

  get rule() {
    return this._rule;
  }

  get outputCol() {
    return this._outputCol;
  }

  getInputCol(fields) {
    return this.getCol(fields, "inputColumn");
  }

  getCol(fields, columnName) {
    const splitPath = this.rule[columnName].split(".");
    return splitPath.reduce((acc, path, index) => {
      const element = acc.find(field => field.name === path);
      const children = element.children;
      return (children && children.length > 0 && splitPath.length > index + 1) ? children : element;
    }, fields);
  }

  addNewField(fields, newField) {
    const splitPath = this.rule.outputColumn.split(".");
    return splitPath.reduce((acc, path, index) => {
      let element = acc.find(field => field.name === path);
      if (!element) {
        element = (index === splitPath.length - 1) ? newField : new SchemaField(path, splitPath.slice(0, index).join(","), "struct", true, []);
        acc.push(element);
      }
      let ch = element.children;
      if (!ch) {
        element.children = [];
      }
      return element.children;
    }, fields);
  }

}

class CastingConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const inputCol = this.getInputCol(c);
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, this.rule.outputDataType, inputCol.nullable, []);
      this.addNewField(c, newField);
    });
  }

}

class ConcatenationConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const inputCol = this.getInputCol(c);
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", inputCol.nullable, []);
      this.addNewField(c, newField);
    });
  }

}

class DropConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const splitPath = this.rule.outputColumn.split(".");
      splitPath.reduce((acc, path, index) => {
        const elementIndex = acc.findIndex(field => path === field.name);
        const element = acc[elementIndex];
        const children = element.children;
        if (splitPath.length === index + 1) {
          acc.splice(elementIndex, 1);
        }
        return (children && children.length > 0 && splitPath.length > index + 1) ? children : element;
      }, c);
    });
  }

}

class LiteralConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", false, []);
      this.addNewField(c, newField) //TODO: type inference
    });
  }

}

class MappingConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  getTargetCol(fields) {
    return super.getCol(fields, "targetAttribute");
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      let mappingTablePromise = MappingTableService.getMappingTableVersionPromise(this.rule.mappingTable, this.rule.mappingTableVersion);
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", false, []);

      mappingTablePromise.then(mappingTable => {
        const schemaPromise = SchemaService.getSchemaVersionPromise(mappingTable.schemaName, mappingTable.schemaVersion);
        schemaPromise.then(schema => {
          const targetCol = this.getTargetCol(schema.fields);
          newField.type = targetCol.type;
          newField.children = targetCol.children;
        })
      });
      this.addNewField(c, newField)
    });
  }

}

class NegationConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const inputCol = this.getInputCol(c);
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, inputCol.type, inputCol.nullable, []);
      this.addNewField(c, newField);
    });
  }

}

class SingleColumnConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const inputCol = this.getInputCol(c);
      const child = new SchemaField(this.rule.inputColumnAlias, this.rule.outputColumn, inputCol.type, inputCol.nullable, []);
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "struct", false, [child]);
      this.addNewField(c, newField);
    });
  }

}

class SparkSessionConfConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, c => {
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", false, []);
      this.addNewField(c, newField)
    });
  }

}

class UppercaseConformanceRule extends ConformanceRule {

  constructor(rule) {
    super(rule);
  }

  apply(fields) {
    return ArrayUtils.applyOnCopy(fields, fieldsCopy => {
      const inputCol = this.getInputCol(fields);
      const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", inputCol.nullable, []);
      this.addNewField(fieldsCopy, newField);
    });
  }

}

class SchemaField {

  constructor(name, path, type, nullable, children) {
    this._name = name;
    this._path = path;
    this._type = type;
    this._nullable = nullable;
    this._children = children;
  }

  get name() {
    return this._name;
  }

  set name(value) {
    this._name = value;
  }

  get path() {
    return this._path;
  }

  set path(value) {
    this._path = value;
  }

  get type() {
    return this._type;
  }

  set type(value) {
    this._type = value;
  }

  get nullable() {
    return this._nullable;
  }

  set nullable(value) {
    this._nullable = value;
  }

  get children() {
    return this._children;
  }

  set children(value) {
    this._children = value;
  }
}

class ArrayUtils {

  static applyOnCopy(input, fn) {
    const copy = $.extend(true, [], input);
    fn(copy);
    return copy
  }

}
