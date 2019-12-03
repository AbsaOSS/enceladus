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

package za.co.absa.enceladus.conformance.interpreter

import za.co.absa.enceladus.utils.explode.ExplosionContext

/**
  * This class is used to encapsulate a state of exploded arrays during processing of dynamic conformance steps
  * by interpreters.
  *
  * Interpreters such as MappingRuleInterpretedGroupExplode, ExplosionInterpreted and CollapseInterpreter
  * can change this state.
  */
class ExplosionState (ec: ExplosionContext = ExplosionContext()) {

  var explodeContext: ExplosionContext = ec
}
