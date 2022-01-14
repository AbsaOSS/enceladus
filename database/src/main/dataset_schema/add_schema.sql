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

DROP FUNCTION IF EXISTS dataset_schema.add_schema(TEXT, INTEGER, TEXT, JSONB, TEXT);

CREATE OR REPLACE FUNCTION dataset_schema.add_schema(
    IN i_schema_name        TEXT,
    IN i_schema_version     INTEGER,
    IN i_schema_description TEXT,
    IN i_fields             JSONB,
    IN i_user_name          TEXT,
    OUT status              INTEGER,
    OUT status_text         TEXT,
    OUT id_schema_version   BIGINT
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.add_schema(5)
--      Stores a new version of the schema.
--      The i_schema_version has to be an increment of the latest version of an existing schema or 1
--
-- Parameters:
--      i_schema_name               - name of the schema
--      i_schema_version            - version of the schema
--      i_schema_description        - description of the schema
--      i_fields                    - the fields the schema consist of
--      i_user_name                 - the user who submitted the changes
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      id_schema           - id of the newly created schema record
--
-- Status codes:
--      201     - OK
--      403     - Schema has been disabled
--      404     - Schema version wrong
--      409     - Schema already exists
--      423     - Schema is locked
--
-------------------------------------------------------------------------------
DECLARE
    _latest_version INTEGER;
    _locked         BOOLEAN;
    _disabled       BOOLEAN;
BEGIN
    SELECT dss.schema_latest_version, dss.locked_when IS NOT NULL, dss.disabled_when IS NOT NULL
    FROM dataset_schema.schemas dss
    WHERE dss.schema_name = i_schema_name
    FOR UPDATE
    INTO _latest_version, _locked, _disabled;

    IF NOT found THEN
        -- new schema, lock on stats will prevent racing insert of the same schema
        PERFORM
        FROM stats.entities
        FOR UPDATE;

        _latest_version = 0;
    ELSIF _disabled THEN
        status := 403;
        status_text := 'Schema has been disabled';
        RETURN ;
    ELSIF _locked THEN
        status := 423;
        status_text := 'Schema is locked';
        RETURN;
    END IF;

    IF _latest_version >= i_schema_version THEN
        status := 409;
        status_text := 'Schema already exists';
        RETURN;
    ELSIF _latest_version + 1 < i_schema_version THEN
        status := 404;
        status_text := 'Schema version wrong';
        RETURN;
    END IF;

    INSERT INTO dataset_schema.versions (schema_name, schema_version, schema_description, fields, updated_by)
    VALUES (i_schema_name, i_schema_version, i_schema_description, i_fields, i_user_name)
    RETURNING dataset_schema.versions.id_schema_version
    INTO id_schema_version;

    IF _latest_version = 0 THEN
        INSERT INTO dataset_schema.schemas (schema_name, schema_latest_version, created_by)
        VALUES (i_schema_name, i_schema_version, i_user_name);

        UPDATE stats.entities
        SET schema_count = schema_count + 1;
    ELSE
        UPDATE dataset_schema.schemas
        SET schema_latest_version = i_schema_version
        WHERE schema_name = i_schema_name;
    END IF;

    status := 201;
    status_text := 'OK';
    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset_schema.add_schema(TEXT, INTEGER, TEXT, JSONB, TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset_schema.add_schema(TEXT, INTEGER, TEXT, JSONB, TEXT) TO menas;
