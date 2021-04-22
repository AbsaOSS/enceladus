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

class ConformanceRule {

  constructor(rule) {
    if (this.apply === undefined) {
      throw new TypeError("Abstract function 'apply' not implemented.");
    }

    this._rule = rule;
    this._outputCol = this.getOutputColumnWithPath(rule.outputColumn);
  }

  getOutputColumnWithPath(outputCol) {
    const index = outputCol.lastIndexOf(".");
    const name = (index === -1) ? outputCol : outputCol.slice(index + 1);
    const path = (index === -1) ? "" : outputCol.slice(0, index);
    return {name: name, path: path};
  }

  get rule() {
    return this._rule;
  }

  get outputCol() {
    return this._outputCol;
  }

  getInputCol(fields) {
    return this.getCol(fields, this.rule.inputColumn);
  }

  getCol(fields, columnName) {
    const splitPath = columnName.split(".");
    return splitPath.reduce((fields, path, index) => {
      const field = fields.find(field => field.name === path);
      if (field === undefined) {
        throw new UnknownFiledError(columnName, this.rule.order);
      }
      const children = field.children;
      return (children && children.length > 0 && splitPath.length > index + 1) ? children : field;
    }, fields);
  }

  addNewField(fields, newField) {
    const splitPath = this.rule.outputColumn.split(".");
    return splitPath.reduce((acc, path, index) => {
      let element = acc.find(field => field.name === path);
      if (!element) {
        element = (index === splitPath.length - 1) ? newField : new SchemaField(path, splitPath.slice(0, index).join(","), "struct", true, [], true);
        acc.push(element);
      }
      let ch = element.children;
      if (!ch) {
        element.children = [];
      }
      return element.children;
    }, fields);
  }

  validate(fields, schemas) {
    return this.apply(fields);
  }

}

class CastingConformanceRule extends ConformanceRule {

  apply(fields) {
    const inputCol = this.getInputCol(fields);
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, this.rule.outputDataType, inputCol.nullable, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class ConcatenationConformanceRule extends ConformanceRule {

  apply(fields) {
    const isNullable = this.getInputCols(fields).some(field => field.nullable);
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", isNullable, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

  getInputCols(fields) {
    return this.rule.inputColumns.map(inputCol => this.getCol(fields, inputCol));
  }

}

class DropConformanceRule extends ConformanceRule {

  apply(fields) {
    const splitPath = this.rule.outputColumn.split(".");
    splitPath.reduce((acc, path, index) => {
      const elementIndex = acc.findIndex(field => path === field.name);
      const element = acc[elementIndex];
      if (element === undefined) {
        throw new UnknownFiledError(this.rule.outputColumn, this.rule.order)
      }
      const children = element.children;
      if (splitPath.length === index + 1) {
        acc.splice(elementIndex, 1);
      }
      return (children && children.length > 0 && splitPath.length > index + 1) ? children : element;
    }, fields);
    return fields;
  }

}

class LiteralConformanceRule extends ConformanceRule {

  apply(fields) {
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", false, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class FillNullsConformanceRule extends ConformanceRule {

  apply(fields) {
    const inputCol = this.getInputCol(fields);
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, inputCol.type, false, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class CoalesceConformanceRule extends ConformanceRule {

  apply(fields) {
    const isNullable = this.getInputCols(fields).some(field => field.nullable);
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", isNullable, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

  getInputCols(fields) {
    return this.rule.inputColumns.map(inputCol => this.getCol(fields, inputCol));
  }

}

class MappingConformanceRule extends ConformanceRule {

  apply(fields) {

    return new MappingTableRestDAO().getByNameAndVersionSync(this.rule.mappingTable, this.rule.mappingTableVersion)
      .then(mappingTable => {
        return new SchemaRestDAO().getByNameAndVersionSync(mappingTable.schemaName, mappingTable.schemaVersion);
      })
      .then(schema => {
        for(let output of this.rule.outputColumns) {
          const outputColWithPath = this.getOutputColumnWithPath(output.outputColumn);
          const newField = new SchemaField(output.outputColumn, outputColWithPath.path, "string", false, [], true);
          const targetCol = super.getCol(schema.fields, output.targetAttribute);
          newField.type = targetCol.type;
          newField.children = targetCol.children;

          this.addNewOutputField(fields, newField);
        }

        return fields;
      });
  }

  addNewOutputField(fields, newField) {
    const splitPath = this.rule.outputColumn.split(".");
    return splitPath.reduce((acc, path, index) => {
      let element = (index === splitPath.length - 1) ? newField : new SchemaField(path, splitPath.slice(0, index).join(","), "struct", true, [], true);
      acc.push(element);

      let ch = element.children;
      if (!ch) {
        element.children = [];
      }
      return element.children;
    }, fields);
  }

  validate(fields) {
    if (this.rule.joinConditions !== undefined) {
      this.rule.joinConditions.forEach(join => {
        this.getCol(fields, join.datasetField);
      });
    }
    return this.apply(fields)
  }

}

class NegationConformanceRule extends ConformanceRule {

  apply(fields) {
    const inputCol = this.getInputCol(fields);
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, inputCol.type, inputCol.nullable, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class SingleColumnConformanceRule extends ConformanceRule {

  apply(fields) {
    const inputCol = this.getInputCol(fields);

    const clonedInputChildren = SchemaField.cloneFields(inputCol.children); // otherwise original inputColumn child would be tampered with
    const child = new SchemaField(this.rule.inputColumnAlias, this.rule.outputColumn, inputCol.type, inputCol.nullable, clonedInputChildren, true);
    child.setDeepConformed(true); // otherwise all subfield would not be marked as conformed (= would stay exactly as in inputCol)

    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "struct", false, [child], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class SparkSessionConfConformanceRule extends ConformanceRule {

  apply(fields) {
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", false, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class UppercaseConformanceRule extends ConformanceRule {

  apply(fields) {
    const inputCol = this.getInputCol(fields);
    const newField = new SchemaField(this.outputCol.name, this.outputCol.path, "string", inputCol.nullable, [], true);
    this.addNewField(fields, newField);
    return fields;
  }

}

class SchemaField {

  constructor(name, path, type, nullable, children, conformed = false) {
    this._name = name;
    this._path = path;
    this._type = type;
    this._nullable = nullable;
    this._children = children;
    this._conformed = conformed;
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

  get conformed() {
    return this._conformed;
  }

  set conformed(value) {
    this._conformed = value;
  }

  setDeepConformed(value) {
    console.debug(`Recursively setting conformed=${value} on ${this.path}`);
    SchemaField.setDeepConformed(this, value);
  }

  static setDeepConformed(field, value) {
    field.conformed = value;
    field.children.forEach(child => {
      SchemaField.setDeepConformed(child, value)
    })
  }

  static cloneField(field) {
    const clonedChildren = field.children.map(child => SchemaField.cloneField(child));
    return new SchemaField(field.name, field.path, field.type, field.nullable, clonedChildren, field.conformed);
  }

  static cloneFields(fields) {
      return fields.map(field => SchemaField.cloneField(field));
  }

}
