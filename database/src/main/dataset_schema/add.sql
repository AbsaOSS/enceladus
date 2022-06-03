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

CREATE OR REPLACE FUNCTION dataset_schema.add(
    IN i_entity_name        TEXT,
    IN i_entity_version     INTEGER,
    IN i_entity_description TEXT,
    IN i_fields             JSONB,
    IN i_user_name          TEXT,
    OUT status              INTEGER,
    OUT status_text         TEXT,
    OUT id_entity_version   BIGINT
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.add(5)
--      Stores a new version of the schema.
--      The i_entity_version has to be an increment of the latest version of an existing schema or 1
--
-- Parameters:
--      i_entity_name               - name of the schema
--      i_entity_version            - version of the schema
--      i_entity_description        - description of the schema
--      i_fields                    - the fields the schema consist of
--      i_user_name                 - the user who submitted the changes
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      id_schema           - id of the newly created schema record
--
-- Status codes:
--      11                  - OK
--      31                  - Schema has been disabled
--      32                  - Schema is locked
--      50                  - Schema version wrong
--      51                  - Schema already exists
--
-------------------------------------------------------------------------------
DECLARE
    _entity_type    CHAR := 'S';
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
    FROM dataset_schema.entities E
    WHERE E.entity_name = i_entity_name
    FOR UPDATE
    INTO _key_entity, _latest_version, _locked, _disabled;

    _new_entity := NOT found;

    IF _new_entity THEN
        IF i_entity_version != 1 THEN
            status := 50;
            status_text := 'Schema version wrong';
            RETURN;
        END IF;

        UPDATE entity_base.stats
        SET entity_count = stats.entity_count + 1
        WHERE entity_type = _entity_type;

        INSERT INTO dataset_schema.entities (entity_name, entity_latest_version, created_by)
        VALUES (i_entity_name, i_entity_version, i_user_name)
        RETURNING id_entity
        INTO _key_entity;
    ELSE
        IF _disabled THEN
            status := 31;
            status_text := 'Schema has been disabled';
            RETURN ;
        ELSIF _locked THEN
            status := 32;
            status_text := 'Schema is locked';
            RETURN;
        ELSEIF _latest_version >= i_entity_version THEN
            status := 51;
            status_text := 'Schema already exists';
            RETURN;
        ELSIF _latest_version + 1 < i_entity_version THEN
            status := 50;
            status_text := 'Schema version wrong';
            RETURN;
        END IF;
    END IF;

    INSERT INTO dataset_schema.versions (key_entity, entity_version, entity_description, fields, updated_by)
    VALUES (_key_entity, i_entity_version, i_entity_description, i_fields, i_user_name)
    RETURNING dataset_schema.versions.id_entity_version
    INTO id_entity_version;

    IF NOT _new_entity THEN
        UPDATE dataset_schema.entities
        SET entity_latest_version = i_entity_version
        WHERE id_entity = _key_entity;
    END IF;

    status := 11;
    status_text := 'OK';
    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset_schema.add(TEXT, INTEGER, TEXT, JSONB, TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset_schema.add(TEXT, INTEGER, TEXT, JSONB, TEXT) TO menas;
