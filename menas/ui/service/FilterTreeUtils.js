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

class FilterTreeUtils {

  static #applyToFilterData(filterData, applyFn, mutable) {
    if (!filterData) {
      return filterData; // when empty, return as-is
    }

    // recursive function to apply on all level of the tree data
    const recursiveFnWrapper = function(filterNode) {
      applyFn(filterNode);

      // recursively do the same:
      // AndJoinedFilters, OrJoinedFilters have field `filterItems` defined; NotFilter has field `inputFilter` defined.
      if(filterNode.filterItems) filterNode.filterItems.forEach(recursiveFnWrapper);
      if(filterNode.inputFilter) recursiveFnWrapper(filterNode.inputFilter);
    };

    // if the method is to be pure from the outside: making a deep copy to do the changes on at first:
    let filterDataNode = mutable ? filterData : jQuery.extend(true, { }, filterData);
    recursiveFnWrapper(filterDataNode); // apply recursive changes mutably

    return filterDataNode;
  }


  /**
   * Aux method to apply a function recursively to all nodes in all levels of the filterData structure (immutable fn'l way)
   * @param filterData root node
   * @param applyFn function to apply (expected to be mutating)
   * @returns copy of the `filterData` root node with changes applied
   */
  static applyToFilterDataImmutably(filterData, applyFn) {
    return this.#applyToFilterData(filterData, applyFn, false)
  }

  static applyToFilterDataMutably(filterData, applyFn) {
    return this.#applyToFilterData(filterData, applyFn, true)
  }

  // simple spark-sql types for hinting, origin: https://spark.apache.org/docs/latest/sql-ref-datatypes.htm
  static columnTypeNames = [
    "boolean", "byte", "short", "integer", "long", "bigint", "float", "double", "decimal", "numeric",
    "date", "timestamp", "string", "binary", "interval"
  ]

}
