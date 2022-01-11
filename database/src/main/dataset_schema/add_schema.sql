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
    OUT id_schema           BIGINT
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
--      403     - Schema already exists
--      404     - Schema version wrong
--      405     - Schema has been deleted
--      406     - Schema is locked
--
-------------------------------------------------------------------------------
DECLARE
    _latest_version INTEGER;
    _locked         BOOLEAN;
    _deleted        BOOLEAN;
BEGIN
    SELECT dsh.schema_latest_version, dsh.locked_when IS NOT NULL, dsh.deleted_when IS NOT NULL
    FROM dataset_schema.heads dsh
    WHERE dsh.schema_name = i_schema_name
    FOR UPDATE
    INTO _latest_version, _locked, _deleted;

    IF NOT found THEN
        -- new schema, lock on stats will prevent racing insert of the same schema
        PERFORM
        FROM stats.jobs_configurations
        FOR UPDATE;

        _latest_version = 0;
    ELSIF _deleted THEN
        status := 405;
        status_text := 'Schema has been deleted';
        RETURN ;
    ELSIF _locked THEN
        status := 406;
        status_text := 'Schema is locked';
        RETURN;
    END IF;

    IF _latest_version >= i_schema_version THEN
        status := 403;
        status_text := 'Schema already exists';
        RETURN;
    ELSIF _latest_version + 1 < i_schema_version THEN
        status := 404;
        status_text := 'Schema version wrong';
        RETURN;
    END IF;

    INSERT INTO dataset_schema.schemas (schema_name, schema_version, schema_description, fields, updated_by)
    VALUES (i_schema_name, i_schema_version, i_schema_description, i_fields, i_user_name)
    RETURNING id_schema
    INTO id_schema;

    IF _latest_version = 0 THEN
        INSERT INTO dataset_schema.heads (schema_name, schema_latest_version, created_by)
        VALUES (i_schema_name, i_schema_version, i_user_name);

        UPDATE stats.jobs_configurations
        SET schema_count = schema_count + 1;
    ELSE
        UPDATE dataset_schema.heads
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
