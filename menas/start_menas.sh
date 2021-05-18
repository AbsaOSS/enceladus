#!/bin/sh

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

if [ -n "${PRIVATE_KEY}" ] && [ -n "${CERTIFICATE}" ]; then
  envsubst < ./nginx.conf > /etc/nginx/nginx.conf
  echo "${PRIVATE_KEY}" >> /etc/ssl/private.pem
  echo "${CERTIFICATE}" >> /etc/ssl/certificate.pem
  echo "${CA_CHAIN}" >> /etc/ssl/cachain.pem
fi

envsubst < /usr/share/nginx/html/package.json > /usr/share/nginx/html/package-new.json
rm /usr/share/nginx/html/package.json
mv /usr/share/nginx/html/package-new.json /usr/share/nginx/html/package.json
