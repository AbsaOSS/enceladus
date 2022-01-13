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

-- DROP TABLE IF EXISTS stats.entities

CREATE TABLE stats.entities
(
    schema_count        INTEGER NOT NULL,
    mapping_table_count INTEGER NOT NULL,
    dataset_count       INTEGER NOT NULL
);

ALTER TABLE stats.entities
    OWNER to enceladus;

INSERT INTO stats.entities (schema_count, mapping_table_count, dataset_count)
VALUES (0, 0, 0);

CREATE RULE entities_del_protect AS ON DELETE TO stats.entities DO INSTEAD NOTHING;
CREATE RULE entities_ins_protect AS ON INSERT TO stats.entities DO INSTEAD NOTHING;
