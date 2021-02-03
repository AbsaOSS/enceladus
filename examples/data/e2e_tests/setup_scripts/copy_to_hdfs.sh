#!/bin/bash

# Copyright 2021 ABSA Group Limited
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

SRC_DIR=$(dirname "$0")
DATA_DIR="${SRC_DIR}/../data/hdfs"
STD_DIR="${DATA_DIR}/std"
REF_DIR="${DATA_DIR}/ref"
PUBLISH_DIR="${DATA_DIR}/publish"
TARGET_DIR=""
TO_CREATE=""

if [ $1 != "with" ]; then
  read -p 'HDFS root folder for data: ' TARGET_DIR
else
  if [ "$2" = "defaults" ]; then
    TARGET_DIR="" # Will to root
    TO_CREATE="n" # No need to create
  else
    TARGET_DIR=$2
    TO_CREATE="${3:-n}" # 3 param optional
  fi
fi

echo "Checking HDFS for ${TARGET_DIR}"
hdfs dfs -ls "$TARGET_DIR" > /dev/null 2>&1

if [ $? != 0 ]; then
  echo "${TARGET_DIR} folder does not exist."

  if [ $1 != "with" ]; then
    read -n1 -r -p "Create? (y/n): " TO_CREATE
  fi

  if [ "$TO_CREATE" = "y" ]; then
    echo "Creating folder ${TARGET_DIR} on HDFS"
    hdfs dfs -mkdir -p "$TARGET_DIR"
  else
    exit 0
  fi
fi

hdfs dfs -put "$STD_DIR" "$TARGET_DIR"/std
hdfs dfs -put "$REF_DIR" "$TARGET_DIR"/ref
hdfs dfs -put "$PUBLISH_DIR" "$TARGET_DIR"/publish