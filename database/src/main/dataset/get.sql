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

CREATE OR REPLACE FUNCTION dataset.get(
    IN  i_entity_name           TEXT,
    IN  i_entity_version        INTEGER DEFAULT NULL,
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
    OUT source_path             TEXT,
    OUT publish_path            TEXT,
    OUT key_schema              BIGINT,
    OUT schema_name             TEXT,
    OUT schema_version          INTEGER,
    OUT schema_fields           JSON,
    OUT conformance             JSON[]
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get(2)
--      Returns the data of the requested dataset, based on its name and version
--      If the version is omitted/NULL the latest version data are returned.
--
-- Parameters:
--      i_entity_name       - name of the dataset
--      i_entity_version    - dataset version to return, latest is taken if NULL
--
-- Returns:
--      status                  - Status code
--      status_text             - Status text
--      id_schema               - Id of the schema
--      entity_name             - name of the schema
--      entity_version          - the version of the schema
--      entity_description      - description of the schema
--      created_by              - user who created the schema
--      created_at              - time & date when the schema was disabled
--      updated_by              - user who updated the schema to this particular version
--      updated_at              - time & date when the this particular version of the schema was created
--      locked_by               - if locked, who was the user who locked the schema
--      locked_at               - if not NULL the schema is locked
--      disabled_by             - if disabled, who was the user who disabled the schema
--      disabled_at             - if not NULL the schema has been disabled
--      source_path             - source path of the dataset
--      publish_path            - publish path of the dataset
--      key_schema              - id of the attached schema
--      schema_name             - name of the schema
--      schema_version          - the version of the schema
--      schema_fields           - the fields the schema consist of
--      conformance             - conformance rules of the dataset
--
-- Status codes:
--      10                  - OK
--      40                  - Dataset does not exist
--      42                  - Schema not found (Should never happen)
--      43                  - Dataset of the given version does not exist
--
-------------------------------------------------------------------------------
DECLARE
    _entity_version     INTEGER;
    _schema_status      INTEGER;
BEGIN
    SELECT coalesce(i_entity_version, E.entity_latest_version), E.created_by, E.created_at,
           E.locked_by, E.locked_at, E.disabled_by, E.locked_at
    FROM dataset.entities E
    WHERE E.entity_name = i_entity_name
    INTO _entity_version, get.created_by, get.created_at,
        get.locked_by, get.locked_at, get.disabled_by, get.disabled_at;

    IF NOT found THEN
        status := 40;
        status_text := 'Dataset does not exist';
        RETURN;
    END IF;

    SELECT 10, 'OK', V.id_entity_version, V.entity_name, V.entity_version,
        V.entity_description, V.updated_by, V.updated_at,
        V.source_path, V.publish_path, V.key_schema, V.conformance
    FROM dataset.versions V
    WHERE V.entity_name = i_entity_name AND
        V.entity_version = _entity_version
    INTO status, status_text, get.id_entity_version, get.entity_name, get.entity_version,
        get.entity_description, get.updated_by, get.updated_at,
        get.source_path, get.publish_path, get.key_schema, get.conformance;

    IF NOT found THEN
        status := 43;
        status_text := 'Dataset of the given version does not exist';
        RETURN;
    END IF;

    SELECT G.status, G.entity_name, G.entity_version, G.fields
    FROM dataset_schema.get(key_schema) G
    INTO _schema_status, schema_name, schema_version, schema_fields;

    IF _schema_status != 10 THEN
        status := 42;
        status_text := 'Schema not found (Should never happen)';
        RETURN;
    END IF;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;


CREATE OR REPLACE FUNCTION dataset.get(
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
    OUT source_path             TEXT,
    OUT publish_path            TEXT,
    OUT key_schema              BIGINT,
    OUT schema_name             TEXT,
    OUT schema_version          INTEGER,
    OUT schema_fields           JSON,
    OUT conformance             JSON[]
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.get(1)
--      Returns the data of the requested dataset, based on its id
--
-- Parameters:
--      i_key_schema            - id of the dataset
--
-- Returns:
--      status                  - Status code
--      status_text             - Status text
--      id_schema               - Id of the schema
--      entity_name             - name of the schema
--      entity_version          - the version of the schema
--      entity_description      - description of the schema
--      created_by              - user who created the schema
--      created_at              - time & date when the schema was disabled
--      updated_by              - user who updated the schema to this particular version
--      updated_at              - time & date when the this particular version of the schema was created
--      locked_by               - if locked, who was the user who locked the schema
--      locked_at               - if not NULL the schema is locked
--      disabled_by             - if disabled, who was the user who disabled the schema
--      disabled_at             - if not NULL the schema has been disabled
--      source_path             - source path of the dataset
--      publish_path            - publish path of the dataset
--      key_schema              - id of the attached schema
--      schema_name             - name of the schema
--      schema_version          - the version of the schema
--      schema_fields           - the fields the schema consist of
--      conformance             - conformance rules of the dataset
--
-- Status codes:
--      10                  - OK
--      40                  - Dataset does not exist
--      42                  - Schema not found (Should never happen)
--
-------------------------------------------------------------------------------
DECLARE
    _schema_status  TEXT;
BEGIN

    SELECT 10, 'OK', V.id_entity_version, V.entity_name, V.entity_version,
           V.entity_description, V.updated_by, V.updated_at,
           V.source_path, V.publish_path, V.key_schema, V.conformance
    FROM dataset.versions V
    WHERE V.id_entity_version = i_key_entity_version
    INTO status, status_text, get.id_entity_version, get.entity_name, get.entity_version,
        get.entity_description, get.updated_by, get.updated_at,
        get.source_path, get.publish_path, get.key_schema, get.conformance;

    IF NOT found THEN
        status := 40;
        status_text := 'Dataset does not exist';
        RETURN;
    END IF;


    SELECT E.created_by, E.created_at, E.locked_by, E.locked_at,
        E.disabled_by, E.locked_at
    FROM dataset.entities E
    WHERE E.entity_name = get.entity_name
    INTO get.created_by, get.created_at, get.locked_by, get.locked_at,
        get.disabled_by, get.disabled_at;

    SELECT G.status, G.entity_name, G.entity_version, G.fields
    FROM dataset_schema.get(key_schema) G
    INTO _schema_status, schema_name, schema_version, schema_fields;

    IF _schema_status != 10 THEN
        status := 42;
        status_text := 'Schema not found (Should never happen)';
        RETURN;
    END IF;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset.get(TEXT, INTEGER) OWNER TO enceladus;
ALTER FUNCTION dataset.get(BIGINT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset.get(TEXT, INTEGER) TO menas;
GRANT EXECUTE ON FUNCTION dataset.get(BIGINT) TO menas;
