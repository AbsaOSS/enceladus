# To build, first build menas and be in enceladus/menas folder. Then run:
# $> docker build -t menas .
# Run image using:
# $> docker run \
#       -e JAVA_OPTS=' \
#           -Dmenas.mongo.connection.string=mongodb://host.docker.internal:27017 \
#           -Dmenas.mongo.connection.database=menas \
#       -p 8080:8080 \
#       menas
FROM tomcat:9-jre8-alpine

LABEL \
    vendor="AbsaOSS" \
    copyright="2020 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Menas Web"

EXPOSE 8080
EXPOSE 8009

RUN rm -rf /usr/local/tomcat/webapps/*
COPY target/*.war /usr/local/tomcat/webapps/ROOT.war
