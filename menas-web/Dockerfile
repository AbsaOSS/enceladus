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

ARG DOCKER_BASE_ARTIFACT=nginx
ARG DOCKER_BASE_TAG=1-alpine

FROM ${DOCKER_BASE_ARTIFACT}:${DOCKER_BASE_TAG}

LABEL \
    vendor="AbsaOSS" \
    copyright="2020 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Menas Web"

ADD start_menas_web.sh /docker-entrypoint.d/start_menas_web.sh
COPY nginx.conf nginx.conf
RUN chmod +x /docker-entrypoint.d/start_menas_web.sh && rm -rf /usr/share/nginx/html/*

COPY ./ui/dist /usr/share/nginx/html/
