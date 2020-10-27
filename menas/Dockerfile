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
    name="Menas"

ADD src/main/resources/start_menas.sh start_menas.sh
RUN chmod +x start_menas.sh && \
    rm -rf webapps/*

ADD ./target/*.war webapps/ROOT.war
ADD ./src/main/resources/server.xml conf/server.xml

EXPOSE 8443
EXPOSE 8009

CMD ["./start_menas.sh"]
