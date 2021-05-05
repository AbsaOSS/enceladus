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

# To build, first build rest-api and be in enceladus/rest-api folder. Then run:
# $> docker build -t rest-api .
# Run image using:
# $> docker run \
#       -e MENAS_MONGO_CONNECTION_STRING=mongodb://host.docker.internal:27017 \
#       -e MENAS_MONGO_CONNECTION_DATABASE=rest_api \
#       -p 8080:8080 \
#       rest-api

ARG DOCKER_BASE_ARTIFACT=tomcat
ARG DOCKER_BASE_TAG=9-jre8-alpine

FROM ${DOCKER_BASE_ARTIFACT}:${DOCKER_BASE_TAG}

LABEL \
    vendor="AbsaOSS" \
    copyright="2020 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Menas"

ARG WAR_FILE
ARG PROPERTY_FILE

ENV SPRING_CONFIG_NAME=${PROPERTY_FILE:-"application"}

ADD ./src/main/resources/docker/start_rest_api.sh start_rest_api.sh
RUN chmod +x start_rest_api.sh && \
    rm -rf webapps/*

ADD ./target/${WAR_FILE} webapps/ROOT.war
ADD src/main/resources/docker/server.xml /tmp/server.xml

ENV MENAS_ENVIRONMENT=Dev

ENV MENAS_MONGO_CONNECTION_STRING="mongodb://localhost:27017"
ENV MENAS_MONGO_CONNECTION_DATABASE=menas

ENV MENAS_HADOOP_AUTH_METHOD=default

ENV MENAS_SPARK_MASTER="local[1]"
ENV SPLINE_URLTEMPLATE="//localhost:8080/spline/dataset/lineage/_search?path=%s&application_id=%s"

ENV MENAS_AUTH_MECHANISM=inmemory
ENV MENAS.AUTH.JWT.SECRET="u7w!z%C*F-JaNdRgUkXp2s5v8y/A?D(G+KbPeShVmYq3t6w9z$C&E)H@McQfTjWn"
ENV MENAS_AUTH_INMEMORY_USER=user
ENV MENAS_AUTH_INMEMORY_PASSWORD=changeme

ENV MENAS_AUTH_ADMIN_ROLE=ROLE_ADMIN
ENV MENAS_AUTH_INMEMORY_ADMIN_USER=menas_admin
ENV MENAS_AUTH_INMEMORY_ADMIN_PASSWORD=admin123

ENV HADOOP_CONF_DIR=/tmp

EXPOSE 8080
EXPOSE 8443
EXPOSE 8009
#Debugging
#EXPOSE 5005
CMD ["./start_rest_api.sh"]
