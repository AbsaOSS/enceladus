FROM envoyproxy/envoy-alpine:v1.16.0

ADD start_envoy.sh /start_envoy.sh
ADD envoy.yaml /etc/envoy.yaml

RUN chmod +x /start_envoy.sh

ENTRYPOINT ["/bin/sh"]

EXPOSE 443

CMD ["start_envoy.sh"]