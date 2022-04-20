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

CREATE OR REPLACE FUNCTION mapping_table.add(
    IN  i_entity_name               TEXT,
    IN  i_entity_version            INTEGER,
    IN  i_entity_description        TEXT,
    IN  i_table_path                TEXT,
    IN  i_key_schema                BIGINT,
    IN  i_default_mapping_values    HSTORE,
    IN  i_table_filter              JSON,
    IN  i_user_name                 TEXT,
    OUT status                      INTEGER,
    OUT status_text                 TEXT,
    OUT key_entity_version          BIGINT
) RETURNS record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.add(8)
--      Stores a new version of the mapping table.
--      The i_entity_version has to be an increment of the latest version of an existing mapping table or 1
--
-- Parameters:
--      i_entity_name               - name of the mapping table
--      i_entity_version            - version of the mapping table
--      i_entity_description        - description of the mapping table
--      i_table_path                - table_path, where the mapping table data are saved
--      i_key_schema                - reference to the schema of the mapping table
--      i_default_mapping_values    - default values of the mapping table
--      i_table_filter              - filter on the data of the mapping table
--      i_user_name                 - the user who submitted the changes
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      key_entity_version  - id of the newly created mapping table record
--
-- Status codes:
--      11                  - OK
--      31                  - Mapping table has been disabled
--      32                  - Mapping table is locked
--      42                  - Schema does not exists
--      50                  - Mapping table version wrong
--      51                  - Mapping table already exists
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
    FROM mapping_table._add(i_entity_name, i_entity_version, i_entity_description, i_table_path,
                            i_key_schema, i_default_mapping_values, i_table_filter, i_user_name) A
    INTO status, status_text, key_entity_version;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION mapping_table.add(TEXT, INTEGER, TEXT, TEXT, BIGINT, HSTORE, JSON, TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION mapping_table.add(TEXT, INTEGER, TEXT, TEXT, BIGINT, HSTORE, JSON, TEXT) TO menas;

CREATE OR REPLACE FUNCTION mapping_table.add(
    IN  i_entity_name               TEXT,
    IN  i_entity_version            INTEGER,
    IN  i_entity_description        TEXT,
    IN  i_table_path                TEXT,
    IN  i_schema_name               TEXT,
    IN  i_schema_version            INTEGER,
    IN  i_default_mapping_values    HSTORE,
    IN  i_table_filter              JSON,
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
--      The i_entity_version has to be an increment of the latest version of an existing mapping table or 1
--
-- Parameters:
--      i_entity_name               - name of the mapping table
--      i_entity_version            - version of the mapping table
--      i_entity_description        - description of the mapping table
--      i_table_path                - table_path, where the mapping table data are saved
--      i_schema_name               - name of the referenced schema of the mapping table
--      i_schema_version            - version of the referenced schema of the mapping table
--      i_default_mapping_values    - default values of the mapping table
--      i_table_filter              - filter on the data of the mapping table
--      i_user_name                 - the user who submitted the changes
--
-- Returns:
--      status              - Status code
--      status_text         - Status text
--      key_entity_version  - id of the newly created mapping table record
--
-- Status codes:
--      11                  - OK
--      31                  - Mapping table has been disabled
--      32                  - Mapping table is locked
--      42                  - Schema does not exists
--      50                  - Mapping table version wrong
--      51                  - Mapping table already exists
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
    FROM mapping_table._add(i_entity_name, i_entity_version, i_entity_description, i_table_path,
                            _key_schema, i_default_mapping_values, i_table_filter, i_user_name) A
    INTO status, status_text, key_entity_version;

    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION mapping_table.add(TEXT, INTEGER, TEXT, TEXT, TEXT, INTEGER, HSTORE, JSON, TEXT) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION mapping_table.add(TEXT, INTEGER, TEXT, TEXT, TEXT, INTEGER, HSTORE, JSON, TEXT) TO menas;
