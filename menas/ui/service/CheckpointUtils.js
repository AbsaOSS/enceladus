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

class CheckpointUtils {

  static checkpointsControlsMatch(aCheckpoints) {
    if (!Array.isArray(aCheckpoints)) {
      return false
    }

    let firstCheckpointControls = null
    for (let oCheckpoint of aCheckpoints) {
      let aControls = oCheckpoint["controls"]
      if (firstCheckpointControls === null) {
        // set firstCheckpointControls
        if (CheckpointUtils.isValidControlArray(aControls)) {
          firstCheckpointControls = aControls;
        } else {
          return false
        }
      } else {
        // compare to firstCheckpoint
        if (!CheckpointUtils.controlArraysMatch(firstCheckpointControls, aControls)) {
          return false
        }
      }
    }
    return true;
  }

  static controlArraysMatch(aControls1, aControls2) {
    if (aControls1.length != aControls2.length) {
      return false
    }

    for (let i = 0; i < aControls1.length; i++) {
      if (!CheckpointUtils.controlsMatch(aControls1[i], aControls2[i])) {
        return false
      }
    }
    return true;
  }

  static controlsMatch(oControl1, oControl2) {
    return ( oControl1["controlName"] == oControl2["controlName"]
      && oControl1["controlValue"] == oControl2["controlValue"]
    )
  }

  static isValidControl(oControl) {
    return (oControl["controlName"] != null
      && oControl["controlType"] != null
      && oControl["controlCol"] != null
      && oControl["controlValue"] != null
    )
  }

  static isValidControlArray(aControls){
    if (!Array.isArray(aControls)) {
      return false
    }
    for (let oControl of aControls) {
      if (!CheckpointUtils.isValidControl(oControl)) {
        return false
      }
    }
    return true;
  }

}
