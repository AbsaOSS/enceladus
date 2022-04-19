/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- DROP TABLE IF EXISTS dataset.versions;

CREATE TABLE dataset.versions
(
    source_path     TEXT NOT NULL,
    publish_path    TEXT NOT NULL,
    key_schema      BIGINT NOT NULL,
    conformance     JSON[] NOT NULL,
    CONSTRAINT versions_pk PRIMARY KEY (id_entity_version)
)
    INHERITS (entity_base.versions);

ALTER TABLE dataset.versions
    ADD CONSTRAINT versions_unq UNIQUE (entity_name, entity_version);

ALTER TABLE dataset.versions OWNER to enceladus;
