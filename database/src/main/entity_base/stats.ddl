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

-- DROP TABLE IF EXISTS entity_base.stats

CREATE TABLE entity_base.stats
(
    entity_type         CHAR NOT NULL,
    entity_count        INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT stats_pk PRIMARY KEY (entity_type)
);

ALTER TABLE entity_base.stats
    OWNER to enceladus;

INSERT INTO entity_base.stats(entity_type)
VALUES ('S'),
       ('M'),
       ('D');
