#!/bin/bash
if [[ -n ${PRIVATE_KEY} && -n ${CERTIFICATE} && -n ${CA_CHAIN} ]]; then
    echo "Certificate, chain and private key present, running secured version"
    echo "${PRIVATE_KEY}" >> conf/private.pem
    echo "${CERTIFICATE}" >> conf/certificate.pem
    echo "${CA_CHAIN}" >> conf/cachain.pem
    rm conf/server.xml
    cp /tmp/server.xml conf/server.xml
fi
catalina.sh run
