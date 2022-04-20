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

CREATE OR REPLACE FUNCTION dataset._add(
    IN  i_entity_name               TEXT,
    IN  i_entity_version            INTEGER,
    IN  i_entity_description        TEXT,
    IN  i_source_path               TEXT,
    IN  i_publish_path              TEXT,
    IN  i_key_schema                BIGINT,
    IN  i_conformance               JSON[],
    IN  i_user_name                 TEXT,
    OUT status                      INTEGER,
    OUT status_text                 TEXT,
    OUT key_entity_version          BIGINT
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration._add(8)
--      Stores a new version of the dataset.
--
-- Parameters:
--      i_entity_name               - name of the dataset
--      i_entity_version            - version of the dataset
--      i_entity_description        - description of the dataset
--      i_source_path               - source path for the dataset
--      i_publish_path              - output path for the dataset
--      i_key_schema                - reference to the schema of the dataset
--      i_conformance               - array of conformance rules
--      i_user_name                 - the user who submitted the changes
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      key_entity_version  - id of the newly created dataset record
--
-- Status codes:
--      11                  - OK
--      31                  - Dataset has been disabled
--      32                  - Dataset is locked
--      50                  - Dataset version wrong
--      51                  - Dataset already exists
--
-------------------------------------------------------------------------------
DECLARE
    _entity_type    CHAR := 'D';
    _key_entity     BIGINT;
    _new_entity     BOOLEAN;
    _latest_version INTEGER;
    _locked         BOOLEAN;
    _disabled       BOOLEAN;
BEGIN
    IF i_entity_version = 1 THEN
        -- lock on stats to prevent competing inserts of new entity
        PERFORM 1
        FROM entity_base.stats S
        WHERE S.entity_type = _entity_type
        FOR UPDATE;
    END IF;

    SELECT E.id_entity, E.entity_latest_version, E.locked_at IS NOT NULL, E.disabled_at IS NOT NULL
    FROM dataset.entities E
    WHERE E.entity_name = i_entity_name
        FOR UPDATE
    INTO _key_entity, _latest_version, _locked, _disabled;

    _new_entity := NOT found;

    IF _new_entity THEN
        IF i_entity_version != 1 THEN
            status := 50;
            status_text := 'Dataset version wrong';
            RETURN;
        END IF;

        UPDATE entity_base.stats
        SET entity_count = stats.entity_count + 1
        WHERE entity_type = _entity_type;

        INSERT INTO dataset.entities(entity_name, entity_latest_version, created_by)
        VALUES (i_entity_name, i_entity_version, i_user_name)
        RETURNING id_entity
        INTO _key_entity;
    ELSE
        IF _disabled THEN
            status := 31;
            status_text := 'Dataset has been disabled';
            RETURN ;
        ELSIF _locked THEN
            status := 32;
            status_text := 'Dataset is locked';
            RETURN;
        ELSIF _latest_version >= i_entity_version THEN
            status := 51;
            status_text := 'Dataset already exists';
            RETURN;
        ELSIF _latest_version + 1 < i_entity_version THEN
            status := 50;
            status_text := 'Dataset version wrong';
            RETURN;
        END IF;

    END IF;

    INSERT INTO dataset.versions(key_entity, entity_version, entity_description, updated_by,
        source_path, publish_path, key_schema, conformance)
    VALUES (_key_entity, i_entity_version, i_entity_description, i_user_name,
        i_source_path, i_publish_path, i_key_schema, i_conformance)
    RETURNING dataset.versions.id_entity_version
        INTO key_entity_version;

    IF NOT _new_entity THEN
        UPDATE dataset.entities
        SET entity_latest_version = i_entity_version
        WHERE id_entity = _key_entity;
    END IF;

    status := 11;
    status_text := 'OK';
    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset._add(TEXT, INTEGER, TEXT, TEXT, TEXT, BIGINT, JSON[], TEXT) OWNER TO enceladus;
