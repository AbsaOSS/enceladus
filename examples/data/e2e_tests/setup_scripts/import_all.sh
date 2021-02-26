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

# Command line for the script itself

SRC_DIR=$(dirname "$0")
DATA_DIR="${SRC_DIR}/../data/exports"
SCHEMA_DIR="${DATA_DIR}/schemas"
DATASET_DIR="${DATA_DIR}/datasets"
MAPPING_TABLE_DIR="${DATA_DIR}/mapping_tables"
USER=""
PASSWORD=""
URL=""
COOKIE_FILE=$(mktemp /tmp/cookie-jar.txt.XXXXXX)
HEADERS_FILE=$(mktemp /tmp/headers.txt.XXXXXX)

if [ "$1" != "with" ]; then
  read -r -p 'Username: ' USER
  read -r -sp 'Password: ' PASSWORD
  echo
  read -r -p 'Base URL: ' URL
else
  if [ "$2" = "defaults" ]; then
    USER="user"
    PASSWORD="changeme"
    URL="http://localhost:8080/menas"
  else
    USER=$2
    PASSWORD=$3
    URL=$4
  fi
fi

import_function () {
    for var in `ls -d ${PWD}/${1}/*`; do
        canonicalPath=$(realpath "${var}")
        curl -v \
            -b $COOKIE_FILE \
            -H "@${HEADERS_FILE}" \
            -H 'Content-Type:application/json' \
            --data "@${canonicalPath}" \
            "${URL}/api/${2}/importItem"
    done
}

curl -v \
    -X POST \
    -c $COOKIE_FILE \
    -D $HEADERS_FILE \
    "$URL/api/login?username=$USER&password=$PASSWORD"

#import_function $SCHEMA_DIR "schema"
#import_function $MAPPING_TABLE_DIR "mappingTable"
import_function $DATASET_DIR "dataset"

rm $COOKIE_FILE
rm $HEADERS_FILE
