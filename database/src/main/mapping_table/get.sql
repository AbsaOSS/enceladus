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

CREATE OR REPLACE FUNCTION mapping_table.get(
    IN  i_mapping_table_name        TEXT,
    IN  i_mapping_table_version     INTEGER DEFAULT NULL,
    OUT status                      INTEGER,
    OUT status_text                 TEXT,
    OUT key_mapping_table_version   BIGINT,
    OUT mapping_table_name          TEXT,
    OUT mapping_table_version       INTEGER,
    OUT mapping_table_description   TEXT,
    OUT path                        TEXT,
    OUT key_schema_version          BIGINT,
    default_mapping_values, table_filter, updated_by
    OUT key_schema_version   BIGINT,

--     OUT schema_name         TEXT,
--     OUT schema_version      INTEGER,
--     OUT schema_description  TEXT,
--     OUT fields              JSONB,
--     OUT created_by          TEXT,
--     OUT created_when        TIMESTAMP WITH TIME ZONE,
--     OUT updated_by          TEXT,
--     OUT updated_when        TIMESTAMP WITH TIME ZONE,
--     OUT locked_by           TEXT,
--     OUT locked_when         TIMESTAMP WITH TIME ZONE,
--     OUT disabled_by         TEXT,
--     OUT disabled_when       TIMESTAMP WITH TIME ZONE
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get(2)
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
--      created_when        - time & date when the schema was disabled
--      updated_by          - user who updated the schema to this particular version
--      updated_when        - time & date when the this particular version of the schema was created
--      locked_by           - if locked, who was the user who locked the schema
--      locked_when         - if not NULL the schema is locked
--      disabled_by         - if disabled, who was the user who disabled the schema
--      disabled_when       - if not NULL the schema has been disabled
--
-- Status codes:
--      10                  - OK
--      40                  - Schema does not exist
--      43                  - Schema of the given version does not exist
--
-------------------------------------------------------------------------------
DECLARE
    _schema_version INTEGER;
    _created_by     TEXT;
    _created_when   TIMESTAMP WITH TIME ZONE;
    _locked_by      TEXT;
    _locked_when    TIMESTAMP WITH TIME ZONE;
    _disabled_by     TEXT;
    _disabled_when   TIMESTAMP WITH TIME ZONE;
BEGIN
    SELECT coalesce(i_schema_version, dss.schema_latest_version), dss.created_by, dss.created_when,
           dss.locked_by, dss.locked_when, dss.disabled_by, dss.locked_when
    FROM mapping_table.schemas dss
    WHERE dss.schema_name = i_schema_name
    INTO _schema_version, _created_by, _created_when,
        _locked_by, _locked_when, _disabled_by, _disabled_when;

    IF NOT found THEN
        status := 40;
        status_text := 'Schema does not exist';
        RETURN;
    END IF;

    SELECT 10, 'OK', dsv.id_schema_version, dsv.schema_name, dsv.schema_version,
           dsv.schema_description, dsv.fields, _created_by, _created_when,
           dsv.updated_by, dsv.updated_when, _locked_by, _locked_when,
           _disabled_by, _disabled_when
    FROM mapping_table.versions dsv
    WHERE dsv.schema_name = i_schema_name AND
          dsv.schema_version = _schema_version
    INTO status, status_text, id_schema_version, schema_name, schema_version,
         schema_description, fields, created_by, created_when,
         updated_by, updated_when, locked_by, locked_when,
         disabled_by, disabled_when;

    IF NOT found THEN
        status := 43;
        status_text := 'Schema of the given version does not exist';
        RETURN;
    END IF;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;


CREATE OR REPLACE FUNCTION mapping_table.get(
    IN  i_key_schema_version    BIGINT,
    OUT status                  INTEGER,
    OUT status_text             TEXT,
    OUT id_schema_version       BIGINT,
    OUT schema_name             TEXT,
    OUT schema_version          INTEGER,
    OUT schema_description      TEXT,
    OUT fields                  JSONB,
    OUT created_by              TEXT,
    OUT created_when            TIMESTAMP WITH TIME ZONE,
    OUT updated_by              TEXT,
    OUT updated_when            TIMESTAMP WITH TIME ZONE,
    OUT locked_by               TEXT,
    OUT locked_when             TIMESTAMP WITH TIME ZONE,
    OUT disabled_by             TEXT,
    OUT disabled_when           TIMESTAMP WITH TIME ZONE
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get(1)
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
--      created_when        - time & date when the schema was disabled
--      updated_by          - user who updated the schema to this particular version
--      updated_when        - time & date when the this particular version of the schema was created
--      locked_by           - if locked, who was the user who locked the schema
--      locked_when         - if not NULL the schema is locked
--      disabled_by         - if disabled, who was the user who disabled the schema
--      disabled_when       - if not NULL the schema has been disabled
--
-- Status codes:
--      10                  - OK
--      40                  - Schema does not exist
--
-------------------------------------------------------------------------------
DECLARE
    _schema_name    TEXT;
BEGIN

    SELECT 10, 'OK', dsv.id_schema_version, dsv.schema_name, dsv.schema_version,
           dsv.schema_description, dsv.fields, dsv.updated_by, dsv.updated_when
    FROM mapping_table.versions dsv
    WHERE dsv.id_schema_version = i_key_schema_version
    INTO status, status_text, id_schema_version, _schema_name, schema_version,
        schema_description, fields, updated_by, updated_when;

    IF NOT found THEN
        status := 40;
        status_text := 'Schema does not exist';
        RETURN;
    END IF;


    SELECT dss.created_by, dss.created_when, dss.locked_by, dss.locked_when,
           dss.disabled_by, dss.locked_when
    FROM mapping_table.schemas dss
    WHERE dss.schema_name = _schema_name
    INTO created_by, created_when, locked_by, locked_when,
         disabled_by, disabled_when;

    schema_name := _schema_name; -- used a local variable to avoid name disambiguity

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION mapping_table.get(TEXT, INTEGER) OWNER TO enceladus;
ALTER FUNCTION mapping_table.get(BIGINT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION mapping_table.get(TEXT, INTEGER) TO menas;
GRANT EXECUTE ON FUNCTION mapping_table.get(BIGINT) TO menas;
