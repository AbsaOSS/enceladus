# Copyright 2018 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM envoyproxy/envoy-alpine:v1.16.0

ADD start_envoy.sh /start_envoy.sh
ADD envoy.yaml /etc/envoy.yaml

RUN chmod +x /start_envoy.sh

ENTRYPOINT ["/bin/sh"]

EXPOSE 443

CMD ["start_envoy.sh"]
