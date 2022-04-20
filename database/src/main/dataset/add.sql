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

CREATE OR REPLACE FUNCTION dataset.add(
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
-- Function: jobs_configuration.add(8)
--      Stores a new version of the dataset.
--      The i_entity_version has to be an increment of the latest version of an existing dataset or 1 in the case of a
--      new one
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
--      42                  - Schema does not exists
--      50                  - Dataset version wrong
--      51                  - Dataset already exists
--
-------------------------------------------------------------------------------
DECLARE
BEGIN
    PERFORM 1
    FROM dataset_schema.versions V
    WHERE V.id_entity_version = i_key_schema;

    IF NOT found THEN
        status := 42;
        status_text := 'Schema does not exists';
        RETURN;
    END IF;

    SELECT A.status, A.status_text, A.key_entity_version
    FROM dataset._add(i_entity_name, i_entity_version, i_entity_description, i_source_path,
        i_publish_path, i_key_schema, i_conformance, i_user_name) A
    INTO status, status_text, key_entity_version;

    RETURN;
END;
$$
    LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset.add(TEXT, INTEGER, TEXT, TEXT, TEXT, BIGINT, JSON[], TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset.add(TEXT, INTEGER, TEXT, TEXT, TEXT, BIGINT, JSON[], TEXT) TO menas;

CREATE OR REPLACE FUNCTION dataset.add(
    IN  i_entity_name               TEXT,
    IN  i_entity_version            INTEGER,
    IN  i_entity_description        TEXT,
    IN  i_source_path               TEXT,
    IN  i_publish_path              TEXT,
    IN  i_schema_name               TEXT,
    IN  i_schema_version            INTEGER,
    IN  i_conformance               JSON[],
    IN  i_user_name                 TEXT,
    OUT status                      INTEGER,
    OUT status_text                 TEXT,
    OUT key_entity_version          BIGINT
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.add(9)
--      Stores a new version of the mapping table.
--      The i_entity_version has to be an increment of the latest version of an existing dataset or 1 in the case of a
--      new one
--
-- Parameters:
--      i_entity_name               - name of the dataset
--      i_entity_version            - version of the dataset
--      i_entity_description        - description of the dataset
--      i_source_path               - source path for the dataset
--      i_publish_path              - output path for the dataset
--      i_schema_name               - name of the referenced schema of the dataset
--      i_schema_version            - version of the referenced schema of the dataset
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
--      42                  - Schema does not exists
--      50                  - Dataset version wrong
--      51                  - Dataset already exists
--
-------------------------------------------------------------------------------
DECLARE
    _key_schema BIGINT;
BEGIN

    SELECT G.id_entity_version
    FROM dataset_schema.get(i_schema_name, i_schema_version) G
    WHERE G.status = 10
    INTO _key_schema;

    IF NOT found THEN
        status := 42;
        status_text := 'Schema does not exists';
        RETURN;
    END IF;

    SELECT A.status, A.status_text, A.key_entity_version
    FROM mapping_table._add(i_entity_name, i_entity_version, i_entity_description, i_source_path,
                            i_publish_path, _key_schema, i_conformance, i_user_name) A
    INTO status, status_text, key_entity_version;

    RETURN;
END;
$$
    LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset.add(TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, INTEGER, JSON[], TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset.add(TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, INTEGER, JSON[], TEXT) TO menas;
