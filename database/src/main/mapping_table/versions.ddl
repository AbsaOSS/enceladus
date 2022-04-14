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

-- DROP TABLE IF EXISTS mapping_table.versions;

CREATE TABLE mapping_table.versions
(
    id_mapping_table_version    BIGINT NOT NULL DEFAULT global_id(),
    mapping_table_name          TEXT NOT NULL,
    mapping_table_version       INTEGER NOT NULL,
    mapping_table_description   TEXT,
    path                        TEXT NOT NULL ,
    key_schema                  BIGINT NOT NULL,
    default_mapping_value       HSTORE,
    filter                      JSONB,
    updated_by                  TEXT NOT NULL,
    updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT versions_pk PRIMARY KEY (id_mapping_table_version)
);

ALTER TABLE mapping_table.versions
    ADD CONSTRAINT versions_unq UNIQUE (mapping_table_name, mapping_table_version);

CREATE INDEX versions_idx ON mapping_table.versions (key_schema);

ALTER TABLE mapping_table.versions OWNER to enceladus;
