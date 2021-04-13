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

if [[ -n ${PRIVATE_KEY} && -n ${CERTIFICATE} && -n ${CA_CHAIN} ]]; then
    echo "Certificate, chain and private key present, running secured version"
    echo "${PRIVATE_KEY}" >> conf/private.pem
    echo "${CERTIFICATE}" >> conf/certificate.pem
    echo "${CA_CHAIN}" >> conf/cachain.pem
    rm conf/server.xml
    cp /tmp/server.xml conf/server.xml
fi
#Debugging
#export CATALINA_OPTS="$CATALINA_OPTS -agentlib:jdwp=transport=dt_socket,address=5005,server=y,suspend=n"
catalina.sh run
