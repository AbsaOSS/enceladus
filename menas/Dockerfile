# To build, first build menas and be in enceladus/menas folder. Then run:
# $> docker build -t menas .
# Run image using:
# $> docker run \
#       -e JAVA_OPTS=' \
#           -Dmenas.mongo.connection.string=mongodb://host.docker.internal:27017 \
#           -Dmenas.mongo.connection.database=menas \
#       -p 8080:8080 \
#       menas
# You can skip the environment properties if you set up your own properties before build
# FROM openjdk:8-jdk-alpine
FROM openjdk:8-jre-alpine

LABEL \
    vendor="AbsaOSS" \
    copyright="2020 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Menas"

ENV TOMCAT_MAJOR=9 \
    TOMCAT_VERSION=9.0.37 \
    TOMCAT_HOME=/opt/tomcat \
    CATALINA_HOME=/opt/tomcat

ENV PATH $CATALINA_HOME/bin:$PATH

EXPOSE 8080
EXPOSE 8009

RUN apk upgrade --update && \
    apk add --update curl && \
    curl -jksSL -o /tmp/apache-tomcat.tar.gz http://archive.apache.org/dist/tomcat/tomcat-${TOMCAT_MAJOR}/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz && \
    gunzip /tmp/apache-tomcat.tar.gz && \
    tar -C /opt -xf /tmp/apache-tomcat.tar && \
    ln -s /opt/apache-tomcat-${TOMCAT_VERSION} ${TOMCAT_HOME} && \
    rm -rf ${TOMCAT_HOME}/webapps/* && \
    apk del curl && \
    rm -rf /tmp/* /var/cache/apk/*

COPY ./target/*.war ${TOMCAT_HOME}/webapps/ROOT.war

CMD ["catalina.sh", "run"]
