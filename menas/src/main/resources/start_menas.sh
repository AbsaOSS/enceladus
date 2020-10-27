#!/bin/sh
echo "${PRIVATE_KEY}" >> conf/private.pem
echo "${CERTIFICATE}" >> conf/certificate.pem
echo "${CA_CHAIN}" >> conf/cachain.pem
catalina.sh run
