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

# To build, first build menas and be in enceladus/menas folder. Then run:
# $> docker build -t menas .
# Run image using:
# $> docker run \
#       -e JAVA_OPTS=' \
#           -Dmenas.mongo.connection.string=mongodb://host.docker.internal:27017 \
#           -Dmenas.mongo.connection.database=menas \
#       -p 8080:8080 \
#       menas

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

ADD ./src/main/resources/docker/start_menas.sh start_menas.sh
RUN chmod +x start_menas.sh && \
    rm -rf webapps/*

ADD ./target/${WAR_FILE} webapps/ROOT.war
ADD src/main/resources/docker/server.xml /tmp/server.xml

ENV MENAS_ENVIRONMENT=Dev

ENV MENAS_MONGO_CONNECTION_STRING="mongodb://localhost:27017"
ENV MENAS_MONGO_CONNECTION_DATABASE=menas

ENV MENAS_HADOOP_AUTH_METHOD=default

ENV MENAS_AUTH_MECHANISM=inmemory
ENV MENAS_AUTH_INMEMORY_USER=user
ENV MENAS_AUTH_INMEMORY_PASSWORD=StrongPassword

ENV MENAS_AUTH_ADMIN_ROLE=ROLE_ADMIN
ENV MENAS_AUTH_INMEMORY_ADMIN_USER=menas_admin
ENV MENAS_AUTH_INMEMORY_ADMIN_PASSWORD=admin123

ENV HADOOP_CONF_DIR=/tmp

EXPOSE 8080
EXPOSE 8443
EXPOSE 8009
#Debugging
#EXPOSE 5005
CMD ["./start_menas.sh"]
