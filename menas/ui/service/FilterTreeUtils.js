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
  /**
   * Aux method to apply a function recursively to all nodes in all levels of the filterData structure (immutable fn'l way)
   * @param filterData root node
   * @param applyFn function to apply (expected to be mutating)
   * @returns copy of the `filterData` root node with changes applied
   */
  static applyToFilterData(filterData, applyFn) {

    // recursive function to apply on all level of the tree data
    const recursiveFn = function(filterNode) {
      applyFn(filterNode);

      // recursively do the same:
      // AndJoinedFilters, OrJoinedFilters have field `filterItems` defined; NotFilter has field `inputFilter` defined.
      if(filterNode.filterItems) filterNode.filterItems.forEach(recursiveFn);
      if(filterNode.inputFilter) recursiveFn(filterNode.inputFilter);
    };

    // the method is pure from the outside: making a deep copy to do the changes on at first:
    let filterDataNode = jQuery.extend(true, { }, filterData);
    recursiveFn(filterDataNode); // apply recursive changes mutably

    return filterDataNode;
  }

}
