#!/bin/bash

# Copyright 2018 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SRC_DIR=`dirname "$0"`

source ${SRC_DIR}/enceladus_const.sh
source ${SRC_DIR}/enceladus_env.sh

export CLASS=${STD_CLASS}

export DEFAULT_DRIVER_MEMORY="$STD_DEFAULT_DRIVER_MEMORY"
export DEFAULT_DRIVER_CORES="$STD_DEFAULT_DRIVER_CORES"
export DEFAULT_EXECUTOR_MEMORY="$STD_DEFAULT_EXECUTOR_MEMORY"
export DEFAULT_EXECUTOR_CORES="$STD_DEFAULT_EXECUTOR_CORES"
export DEFAULT_NUM_EXECUTORS="$STD_DEFAULT_NUM_EXECUTORS"

export DEFAULT_DRA_ENABLED="$STD_DEFAULT_DRA_ENABLED"

export DEFAULT_DRA_MIN_EXECUTORS="$STD_DEFAULT_DRA_MIN_EXECUTORS"
export DEFAULT_DRA_MAX_EXECUTORS="$STD_DEFAULT_DRA_MAX_EXECUTORS"
export DEFAULT_DRA_ALLOCATION_RATIO="$STD_DEFAULT_DRA_ALLOCATION_RATIO"
export DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE="$STD_DEFAULT_ADAPTIVE_TARGET_POSTSHUFFLE_INPUT_SIZE"

source ${SRC_DIR}/run_enceladus.sh
