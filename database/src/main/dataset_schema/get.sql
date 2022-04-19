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

CREATE OR REPLACE FUNCTION dataset_schema.get(
    IN  i_entity_name       TEXT,
    IN  i_entity_version    INTEGER DEFAULT NULL,
    OUT status              INTEGER,
    OUT status_text         TEXT,
    OUT id_entity_version   BIGINT,
    OUT entity_name         TEXT,
    OUT entity_version      INTEGER,
    OUT entity_description  TEXT,
    OUT created_by          TEXT,
    OUT created_at          TIMESTAMP WITH TIME ZONE,
    OUT updated_by          TEXT,
    OUT updated_at          TIMESTAMP WITH TIME ZONE,
    OUT locked_by           TEXT,
    OUT locked_at           TIMESTAMP WITH TIME ZONE,
    OUT disabled_by         TEXT,
    OUT disabled_at         TIMESTAMP WITH TIME ZONE,
    OUT fields              JSON
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get(2)
--      Returns the data of the requested schema, based on its name and version
--      If the version is omitted/NULL the latest version data are returned.
--
-- Parameters:
--      i_entity_name       - name of the schema
--      i_entity_version    - schema version to return, latest is taken if NULL
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      id_schema           - Id of the schema
--      entity_name         - name of the schema
--      entity_version      - the version of the schema
--      entity_description  - description of the schema
--      created_by          - user who created the schema
--      created_at          - time & date when the schema was disabled
--      updated_by          - user who updated the schema to this particular version
--      updated_at          - time & date when the this particular version of the schema was created
--      locked_by           - if locked, who was the user who locked the schema
--      locked_at           - if not NULL the schema is locked
--      disabled_by         - if disabled, who was the user who disabled the schema
--      disabled_at         - if not NULL the schema has been disabled
--      fields              - the fields the schema consist of
--
-- Status codes:
--      10                  - OK
--      40                  - Schema does not exist
--      43                  - Schema of the given version does not exist
--
-------------------------------------------------------------------------------
DECLARE
    _entity_version INTEGER;
    _created_by     TEXT;
    _created_at     TIMESTAMP WITH TIME ZONE;
    _locked_by      TEXT;
    _locked_at      TIMESTAMP WITH TIME ZONE;
    _disabled_by    TEXT;
    _disabled_at    TIMESTAMP WITH TIME ZONE;
BEGIN
    SELECT coalesce(i_entity_version, E.entity_latest_version), E.created_by, E.created_at,
           E.locked_by, E.locked_at, E.disabled_by, E.locked_at
    FROM dataset_schema.entities E
    WHERE E.entity_name = i_entity_name
    INTO _entity_version, _created_by, _created_at  ,
        _locked_by, _locked_at  , _disabled_by, _disabled_at  ;

    IF NOT found THEN
        status := 40;
        status_text := 'Schema does not exist';
        RETURN;
    END IF;

    SELECT 10, 'OK', dsv.id_entity_version, dsv.entity_name, dsv.entity_version,
           dsv.entity_description, dsv.fields, _created_by, _created_at,
           dsv.updated_by, dsv.updated_at  , _locked_by, _locked_at,
           _disabled_by, _disabled_at
    FROM dataset_schema.versions dsv
    WHERE dsv.entity_name = i_entity_name AND
          dsv.entity_version = _entity_version
    INTO status, status_text, get.id_entity_version, get.entity_name, get.entity_version,
         get.entity_description, get.fields, created_by, created_at,
         get.updated_by, get.updated_at, locked_by, locked_at,
         disabled_by, disabled_at;

    IF NOT found THEN
        status := 43;
        status_text := 'Schema of the given version does not exist';
        RETURN;
    END IF;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;


CREATE OR REPLACE FUNCTION dataset_schema.get(
    IN  i_key_entity_version    BIGINT,
    OUT status                  INTEGER,
    OUT status_text             TEXT,
    OUT id_entity_version       BIGINT,
    OUT entity_name             TEXT,
    OUT entity_version          INTEGER,
    OUT entity_description      TEXT,
    OUT created_by              TEXT,
    OUT created_at              TIMESTAMP WITH TIME ZONE,
    OUT updated_by              TEXT,
    OUT updated_at              TIMESTAMP WITH TIME ZONE,
    OUT locked_by               TEXT,
    OUT locked_at               TIMESTAMP WITH TIME ZONE,
    OUT disabled_by             TEXT,
    OUT disabled_at             TIMESTAMP WITH TIME ZONE,
    OUT fields                  JSON
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
--      entity_name         - name of the schema
--      entity_version      - the version of the schema
--      entity_description  - description of the schema
--      created_by          - user who created the schema
--      created_at          - time & date when the schema was disabled
--      updated_by          - user who updated the schema to this particular version
--      updated_at          - time & date when the this particular version of the schema was created
--      locked_by           - if locked, who was the user who locked the schema
--      locked_at           - if not NULL the schema is locked
--      disabled_by         - if disabled, who was the user who disabled the schema
--      disabled_at         - if not NULL the schema has been disabled
--      fields              - the fields the schema consist of
--
-- Status codes:
--      10                  - OK
--      40                  - Schema does not exist
--
-------------------------------------------------------------------------------
DECLARE
BEGIN

    SELECT 10, 'OK', dsv.id_entity_version, dsv.entity_name, dsv.entity_version,
           dsv.entity_description, dsv.fields, dsv.updated_by, dsv.updated_at
    FROM dataset_schema.versions dsv
    WHERE dsv.id_entity_version = i_key_entity_version
    INTO status, status_text, get.id_entity_version, get.entity_name, get.entity_version,
        get.entity_description, get.fields, get.updated_by, get.updated_at;

    IF NOT found THEN
        status := 40;
        status_text := 'Schema does not exist';
        RETURN;
    END IF;


    SELECT E.created_by, E.created_at  , E.locked_by, E.locked_at  ,
           E.disabled_by, E.locked_at
    FROM dataset_schema.entities E
    WHERE E.entity_name = get.entity_name
    INTO get.created_by, get.created_at, get.locked_by, get.locked_at,
         get.disabled_by, get.disabled_at;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset_schema.get(TEXT, INTEGER) OWNER TO enceladus;
ALTER FUNCTION dataset_schema.get(BIGINT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset_schema.get(TEXT, INTEGER) TO menas;
GRANT EXECUTE ON FUNCTION dataset_schema.get(BIGINT) TO menas;
