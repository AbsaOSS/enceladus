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

-- DROP TABLE IF EXISTS mapping_table.mapping_tables;

CREATE TABLE mapping_table.mapping_tables
(
    mapping_table_name              TEXT NOT NULL,
    mapping_table_latest_version    INTEGER NOT NULL,
    created_by                      TEXT NOT NULL,
    created_at                      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    locked_by                       TEXT,
    locked_at                       TIMESTAMP WITH TIME ZONE,
    disabled_by                     TEXT,
    disabled_at                     TIMESTAMP WITH TIME ZONE,
    CONSTRAINT mapping_tables_pk PRIMARY KEY (mapping_table_name)
);

ALTER TABLE mapping_table.mapping_tables OWNER to enceladus;
