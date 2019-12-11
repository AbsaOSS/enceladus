#!/bin/bash

# Copyright 2018-2019 ABSA Group Limited
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

source ${SRC_DIR}/enceladus_env.sh

export CLASS=${STD_CLASS}
export JAR=${STD_JAR}

export DEFAULT_DRIVER_MEMORY="$STD_DEFAULT_DRIVER_MEMORY"
export DEFAULT_DRIVER_CORES="$STD_DEFAULT_DRIVER_CORES"
export DEFAULT_EXECUTOR_MEMORY="$STD_DEFAULT_EXECUTOR_MEMORY"
export DEFAULT_EXECUTOR_CORES="$STD_DEFAULT_EXECUTOR_CORES"
export DEFAULT_NUM_EXECUTORS="$STD_DEFAULT_NUM_EXECUTORS"

source ${SRC_DIR}/run_enceladus.sh
