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
