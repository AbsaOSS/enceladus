#!/bin/sh

if [ -n "${PRIVATE_KEY}" ] && [ -n "${CERTIFICATE}" ]; then
  envsubst < ./nginx.conf > /etc/nginx/nginx.conf
  echo "${PRIVATE_KEY}" >> /etc/ssl/private.pem
  echo "${CERTIFICATE}" >> /etc/ssl/certificate.pem
  echo "${CA_CHAIN}" >> /etc/ssl/cachain.pem
fi

envsubst < /usr/share/nginx/html/package.json > /usr/share/nginx/html/package-new.json
rm /usr/share/nginx/html/package.json
mv /usr/share/nginx/html/package-new.json /usr/share/nginx/html/package.json
