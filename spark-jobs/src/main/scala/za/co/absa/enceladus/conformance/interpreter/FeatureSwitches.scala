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

package za.co.absa.enceladus.conformance.interpreter

/**
  * Class bundling together switched for features to be used during Conformance
  * It's a sealed abstract case class to enforce the the provided constructor (apply). This way values can be
  * changed only via the setters. This way enforces setting them by name only instead of by position like it would be in
  * the case of classical constructor.
  *
  * @param experimentalMappingRuleEnabled If true the new explode-optimized conformance mapping rule interpreter will be used
  * @param catalystWorkaroundEnabled      If true the Catalyst optimizer workaround is enabled
  * @param controlFrameworkEnabled        If true sets the checkpoints on the dataset upon conforming
  * @param broadcastStrategyMode          Specifies the mode of operation of the broadcasting strategy for mapping rules
  * @param broadcastMaxSizeMb             Specifies the maximum size of a mapping table for which the broadcasting strategy can be used.
  */
sealed abstract case class FeatureSwitches(
                                            experimentalMappingRuleEnabled: Boolean = false,
                                            catalystWorkaroundEnabled: Boolean = false,
                                            controlFrameworkEnabled: Boolean = false,
                                            broadcastStrategyMode: ThreeStateSwitch = Auto,
                                            broadcastMaxSizeMb: Int = 0
                                          ) {
  private def copy(
                    experimentalMappingRuleEnabled: Boolean = this.experimentalMappingRuleEnabled,
                    catalystWorkaroundEnabled: Boolean = this.catalystWorkaroundEnabled,
                    controlFrameworkEnabled: Boolean = this.controlFrameworkEnabled,
                    broadcastStrategyMode: ThreeStateSwitch = this.broadcastStrategyMode,
                    broadcastMaxSizeMb: Int = this.broadcastMaxSizeMb
                  ): FeatureSwitches = {
    new FeatureSwitches(experimentalMappingRuleEnabled,
      catalystWorkaroundEnabled,
      controlFrameworkEnabled,
      broadcastStrategyMode,
      broadcastMaxSizeMb) {}
  }

  def setExperimentalMappingRuleEnabled(value: Boolean): FeatureSwitches = {
    copy(experimentalMappingRuleEnabled = value)
  }

  def setCatalystWorkaroundEnabled(value: Boolean): FeatureSwitches = {
    copy(catalystWorkaroundEnabled = value)
  }

  def setControlFrameworkEnabled(value: Boolean): FeatureSwitches = {
    copy(controlFrameworkEnabled = value)
  }

  def setBroadcastStrategyMode(value: ThreeStateSwitch): FeatureSwitches = {
    copy(broadcastStrategyMode = value)
  }

  def setBroadcastMaxSizeMb(value: Int): FeatureSwitches = {
    copy(broadcastMaxSizeMb = value)
  }

}

object FeatureSwitches {
  def apply(): FeatureSwitches = new FeatureSwitches() {}
}
