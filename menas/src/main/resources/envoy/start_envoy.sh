#!/bin/sh

mkdir -p /etc/ssl

echo -e "${PRIVATE_KEY}" >> /etc/ssl/envoy.key
echo -e "${CERTIFICATE}" >> /etc/ssl/envoy.crt
echo -e "${CA_CHAIN}" >> /etc/ssl/envoy.crt

/usr/local/bin/envoy -c /etc/envoy.yaml
