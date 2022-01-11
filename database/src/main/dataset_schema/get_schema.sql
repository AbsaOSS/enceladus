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

DROP FUNCTION dataset_schema.get_schema(TEXT, INTEGER);
DROP FUNCTION dataset_schema.get_schema(BIGINT);

CREATE OR REPLACE FUNCTION dataset_schema.get_schema(
    IN  i_schema_name       TEXT,
    IN  i_schema_version    INTEGER DEFAULT NULL,
    OUT status              INTEGER,
    OUT status_text         TEXT,
    OUT id_schema           BIGINT,
    OUT schema_name         TEXT,
    OUT schema_version      INTEGER,
    OUT schema_description  TEXT,
    OUT fields              JSONB,
    OUT created_by          TEXT,
    OUT created_when        TIMESTAMP WITH TIME ZONE,
    OUT updated_by          TEXT,
    OUT updated_when        TIMESTAMP WITH TIME ZONE,
    OUT locked_by           TEXT,
    OUT locked_when         TIMESTAMP WITH TIME ZONE,
    OUT deleted_by          TEXT,
    OUT deleted_when        TIMESTAMP WITH TIME ZONE
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get_schema(2)
--      Returns the data of the requested schema, based on its name and version
--      If the version is omitted/NULL the latest version data are returned.
--
-- Parameters:
--      i_schema_name       - name of the schema
--      i_schema_version    - schema version to return, latest is taken if NULL
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      id_schema           - Id of the schema
--      schema_name         - name of the schema
--      schema_version      - the version of the schema
--      schema_description  - description of the schema
--      fields              - the fields the schema consist of
--      created_by          - user who created the schema
--      created_when        - time & date when the schema was deleted
--      updated_by          - user who updated the schema to this particular version
--      updated_when        - time & date when the this particular version of the schema was created
--      locked_by           - if locked, who was the user who locked the schema
--      locked_when         - if not NULL the schema is locked
--      deleted_by          - if deleted, who was the user who deleted the schema
--      deleted_when        - if not NULL the schema has been deleted
--
-- Status codes:
--      200     - OK
--      404     - Schema does not exist
--      405     - Schema of the given version does not exist
--
-------------------------------------------------------------------------------
DECLARE
    _schema_version INTEGER;
    _created_by     TEXT;
    _created_when   TIMESTAMP WITH TIME ZONE;
    _locked_by      TEXT;
    _locked_when    TIMESTAMP WITH TIME ZONE;
    _deleted_by     TEXT;
    _deleted_when   TIMESTAMP WITH TIME ZONE;
BEGIN
    SELECT coalesce(i_schema_version, dsh.schema_latest_version), dsh.created_by, dsh.created_when,
           dsh.locked_by, dsh.locked_when, dsh.deleted_by, dsh.locked_when
    FROM dataset_schema.heads dsh
    WHERE dsh.schema_name = i_schema_name
    INTO _schema_version, _created_by, _created_when,
        _locked_by, _locked_when, _deleted_by, _deleted_when;

    IF NOT found THEN
        status := 404;
        status_text := 'Schema does not exist';
        RETURN;
    END IF;

    SELECT 200, 'OK', dss.id_schema, dss.schema_name,
           dss.schema_description, dss.fields, _created_by, _created_when,
           dss.updated_by, dss.updated_when, _locked_by, _locked_when,
           _deleted_by, _deleted_when
    FROM dataset_schema.schemas dss
    WHERE dss.schema_name = i_schema_name AND
          dss.schema_version = _schema_version
    INTO status, status_text, id_schema, schema_name,
         schema_description, fields, created_by, created_when,
         updated_by, updated_when, locked_by, locked_when,
        deleted_by, deleted_when;

    IF NOT found THEN
        status := 405;
        status_text := 'Schema of the given version does not exist';
        RETURN;
    END IF;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;


CREATE OR REPLACE FUNCTION dataset_schema.get_schema(
    IN  i_key_schema        BIGINT,
    OUT status              INTEGER,
    OUT status_text         TEXT,
    OUT id_schema           BIGINT,
    OUT schema_name         TEXT,
    OUT schema_version      INTEGER,
    OUT schema_description  TEXT,
    OUT fields              JSONB,
    OUT created_by          TEXT,
    OUT created_when        TIMESTAMP WITH TIME ZONE,
    OUT updated_by          TEXT,
    OUT updated_when        TIMESTAMP WITH TIME ZONE,
    OUT locked_by           TEXT,
    OUT locked_when         TIMESTAMP WITH TIME ZONE,
    OUT deleted_by          TEXT,
    OUT deleted_when        TIMESTAMP WITH TIME ZONE
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get_schema(1)
--      Returns the data of the requested schema, based on its id
--
-- Parameters:
--      i_key_schema        - id of the schema
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      id_schema           - Id of the schema
--      schema_name         - name of the schema
--      schema_version      - the version of the schema
--      schema_description  - description of the schema
--      fields              - the fields the schema consist of
--      created_by          - user who created the schema
--      created_when        - time & date when the schema was deleted
--      updated_by          - user who updated the schema to this particular version
--      updated_when        - time & date when the this particular version of the schema was created
--      locked_by           - if locked, who was the user who locked the schema
--      locked_when         - if not NULL the schema is locked
--      deleted_by          - if deleted, who was the user who deleted the schema
--      deleted_when        - if not NULL the schema has been deleted
--
-- Status codes:
--      200     - OK
--      404     - Schema does not exist
--
-------------------------------------------------------------------------------
DECLARE
    _schema_name    TEXT;
BEGIN

    SELECT 200, 'OK', dss.id_schema, dss.schema_name,
           dss.schema_description, dss.fields, dss.updated_by, dss.updated_when
    FROM dataset_schema.schemas dss
    WHERE dss.id_schema = i_key_schema
    INTO status, status_text, id_schema, _schema_name,
        schema_description, fields, updated_by, updated_when;

    IF NOT found THEN
        status := 404;
        status_text := 'Schema does not exist';
        RETURN;
    END IF;


    SELECT dsh.created_by, dsh.created_when, dsh.locked_by, dsh.locked_when,
           dsh.deleted_by, dsh.locked_when
    FROM dataset_schema.heads dsh
    WHERE dsh.schema_name = _schema_name
    INTO created_by, created_when, locked_by, locked_when,
         deleted_by, deleted_when;

    schema_name := _schema_name; -- used a local variable to avoid name disambiguaty

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset_schema.get_schema(TEXT, INTEGER) OWNER TO enceladus;
ALTER FUNCTION dataset_schema.get_schema(BIGINT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset_schema.get_schema(TEXT, INTEGER) TO menas;
GRANT EXECUTE ON FUNCTION dataset_schema.get_schema(BIGINT) TO menas;
