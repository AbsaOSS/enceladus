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
    _latest_version INTEGER;
    _locked         BOOLEAN;
    _disabled       BOOLEAN;
BEGIN
    SELECT E.entity_latest_version, E.locked_at IS NOT NULL, E.disabled_at IS NOT NULL
    FROM dataset_schema.entities E
    WHERE E.entity_name = i_entity_name
    FOR UPDATE
    INTO _latest_version, _locked, _disabled;

    IF NOT found THEN
        -- new schema, lock on stats will prevent racing insert of the same schema
        PERFORM
        FROM entity_base.stats
        FOR UPDATE;

        _latest_version = 0;
    ELSIF _disabled THEN
        status := 31;
        status_text := 'Schema has been disabled';
        RETURN ;
    ELSIF _locked THEN
        status := 32;
        status_text := 'Schema is locked';
        RETURN;
    END IF;

    IF _latest_version >= i_entity_version THEN
        status := 51;
        status_text := 'Schema already exists';
        RETURN;
    ELSIF _latest_version + 1 < i_entity_version THEN
        status := 50;
        status_text := 'Schema version wrong';
        RETURN;
    END IF;

    INSERT INTO dataset_schema.versions (entity_name, entity_version, entity_description, fields, updated_by)
    VALUES (i_entity_name, i_entity_version, i_entity_description, i_fields, i_user_name)
    RETURNING dataset_schema.versions.id_entity_version
    INTO id_entity_version;

    IF _latest_version = 0 THEN
        INSERT INTO dataset_schema.entities (entity_name, entity_latest_version, created_by)
        VALUES (i_entity_name, i_entity_version, i_user_name);

        UPDATE entity_base.stats
        SET schema_count = stats.schema_count + 1;
    ELSE
        UPDATE dataset_schema.entities
        SET entity_latest_version = i_entity_version
        WHERE entity_name = i_entity_name;
    END IF;

    status := 11;
    status_text := 'OK';
    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset_schema.add(TEXT, INTEGER, TEXT, JSONB, TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset_schema.add(TEXT, INTEGER, TEXT, JSONB, TEXT) TO menas;
