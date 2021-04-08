FROM nginx:1-alpine

LABEL \
    vendor="AbsaOSS" \
    copyright="2020 ABSA Group Limited" \
    license="Apache License, version 2.0" \
    name="Menas Web"

ADD start_menas_web.sh /docker-entrypoint.d/start_menas_web.sh
COPY nginx.conf nginx.conf
RUN chmod +x /docker-entrypoint.d/start_menas_web.sh && rm -rf /usr/share/nginx/html/*

COPY ./ui/dist /usr/share/nginx/html/
