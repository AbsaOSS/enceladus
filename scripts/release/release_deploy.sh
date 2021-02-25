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

if [[ -z "$GIT_CHECKOUT_DIR" ]]; then
    echo "GIT_CHECKOUT_DIR environment variable has to be specified. It specifies the directory where the release will be executed from."
    exit 1
fi

if [ ! -d "$GIT_CHECKOUT_DIR" ]; then
    echo "Directory specified in GIT_CHECKOUT_DIR environment variable does not exist."
    exit 1
fi

RELEASE_SUBDIR="enceladus_release"

cd "$GIT_CHECKOUT_DIR"

rm -rf $RELEASE_SUBDIR
git clone --single-branch --branch master https://github.com/AbsaOSS/enceladus.git $RELEASE_SUBDIR
cd $RELEASE_SUBDIR
mvn deploy -Ppublic -Psonatype-oss-release -PgenerateComponentPreload -DskipTests
