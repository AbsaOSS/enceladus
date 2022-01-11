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

-- DROP TABLE dataset_schema.schemas;

CREATE TABLE dataset_schema.schemas
(
    id_schema           BIGINT NOT NULL DEFAULT global_id(),
    schema_name         TEXT NOT NULL,
    schema_version      INTEGER NOT NULL,
    schema_description  TEXT,
    fields              JSONB NOT NULL,
    updated_by          TEXT NOT NULL,
    updated_when        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT schemas_pk PRIMARY KEY (id_schema)
);

ALTER TABLE dataset_schema.schemas
    ADD CONSTRAINT schemas_unq UNIQUE (schema_name, schema_version);

ALTER TABLE dataset_schema.schemas OWNER to enceladus;
