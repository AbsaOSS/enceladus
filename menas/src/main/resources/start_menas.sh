#!/bin/bash
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
