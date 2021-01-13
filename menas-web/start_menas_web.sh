#!/bin/sh

envsubst < /usr/share/nginx/html/package.json > /usr/share/nginx/html/package-new.json
rm /usr/share/nginx/html/package.json
mv /usr/share/nginx/html/package-new.json /usr/share/nginx/html/package.json