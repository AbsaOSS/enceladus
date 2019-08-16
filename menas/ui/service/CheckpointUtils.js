class CheckpointUtils {

  static checkpointsContlolsMatch(aCheckpoints) {
    if (!Array.isArray(aCheckpoints)) {
      console.log("aCheckpoints is not an array")
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
          console.log("First checkpoint's controlArray is not valid ")
          return false
        }
      } else {
        // compare to firstCheckpoint
        if (!CheckpointUtils.controlArraysMatch(firstCheckpointControls, aControls)) {
          console.log("Control arrays do not match on checkpoint: " + oCheckpoint["name"])
          return false
        }
      }
    }
    return true;
  }

  static controlArraysMatch(aControls1, aControls2) {
    if (aControls1.length != aControls2.length) {
      console.log("Control arrays length mismatch")
      return false
    }

    for (let i = 0; i < aControls1.length; i++) {
      if (!CheckpointUtils.controlsMatch(aControls1[i], aControls2[i])) {
        console.log("Control mismatch: " + i)
        return false
      }
    }
    return true;
  }

  static controlsMatch(oControl1, oControl2) {
    return ( oControl1["controlName"] == oControl2["controlName"]
      && oControl1["controlType"] == oControl2["controlType"]
      && oControl1["controlCol"] == oControl2["controlCol"]
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
      console.log("aControls is not an array")
      return false
    }
    for (let oControl of aControls) {
      if (!CheckpointUtils.isValidControl(oControl)) {
        console.log("Not a valid control: " + oControl)
        return false
      }
    }
    return true;
  }

}
