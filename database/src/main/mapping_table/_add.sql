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

CREATE OR REPLACE FUNCTION mapping_table._add(
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
-- Function: jobs_configuration._add(8)
--      Stores a new version of the mapping table.
--      The i_mapping table_version has to be an increment of the latest version of an existing mapping table or 1
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
--      50                  - Mapping table version wrong
--      51                  - Mapping table already exists
--
-------------------------------------------------------------------------------
DECLARE
    _latest_version INTEGER;
    _locked         BOOLEAN;
    _disabled       BOOLEAN;
BEGIN
    SELECT E.entity_latest_version, E.locked_at IS NOT NULL, E.disabled_at IS NOT NULL
    FROM mapping_table.entities E
    WHERE E.entity_name = i_entity_name
    FOR UPDATE
    INTO _latest_version, _locked, _disabled;

    IF NOT found THEN
        -- new mapping table, lock on stats will prevent racing insert of the same mapping table
        PERFORM
        FROM entity_base.stats
        FOR UPDATE;

        _latest_version = 0;
    ELSIF _disabled THEN
        status := 31;
        status_text := 'Mapping table has been disabled';
        RETURN ;
    ELSIF _locked THEN
        status := 32;
        status_text := 'Mapping table is locked';
        RETURN;
    END IF;

    IF _latest_version >= i_entity_version THEN
        status := 51;
        status_text := 'Mapping table already exists';
        RETURN;
    ELSIF _latest_version + 1 < i_entity_version THEN
        status := 50;
        status_text := 'Mapping table version wrong';
        RETURN;
    END IF;

    INSERT INTO mapping_table.versions(entity_name, entity_version, entity_description, table_path,
                                       key_schema, default_mapping_values, table_filter, updated_by)
    VALUES (i_entity_name, i_entity_version, i_entity_description, i_table_path,
            i_key_schema, i_default_mapping_values, i_table_filter, i_user_name)
    RETURNING mapping_table.versions.id_entity_version
    INTO key_entity_version;

    IF _latest_version = 0 THEN
        INSERT INTO mapping_table.entities(entity_name, entity_latest_version, created_by)
        VALUES (i_entity_name, i_entity_version, i_user_name);

        UPDATE entity_base.stats
        SET mapping_table_count = mapping_table_count + 1;
    ELSE
        UPDATE mapping_table.entities
        SET entity_latest_version = i_entity_version
        WHERE entity_name = i_entity_name;
    END IF;

    status := 11;
    status_text := 'OK';
    RETURN;
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION mapping_table._add(TEXT, INTEGER, TEXT, TEXT, BIGINT, HSTORE, JSON, TEXT) OWNER TO enceladus;
