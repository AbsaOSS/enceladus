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

-- DROP TABLE IF EXISTS dataset_schema.entities;

CREATE TABLE dataset_schema.entities
(
    entity_type             CHAR NOT NULL DEFAULT 'S',
    CONSTRAINT entities_pk PRIMARY KEY (entity_name)
)
    INHERITS (entity_base.entities);

ALTER TABLE IF EXISTS dataset_schema.entities
    ADD CONSTRAINT check_dataset_schema_entity_type CHECK (entity_type = 'S')
    NOT VALID;

ALTER TABLE dataset_schema.entities OWNER to enceladus;