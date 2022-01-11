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

DROP FUNCTION IF EXISTS dataset_schema.list_schemas(BOOLEAN);

CREATE OR REPLACE FUNCTION dataset_schema.list_schemas(
    IN  i_include_disabled      BOOLEAN DEFAULT FALSE,
    OUT schema_name             TEXT,
    OUT schema_latest_version   INTEGER
) RETURNS SETOF record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.list_schemas(1)
--      Returns a list of schemas with their latest versions
--
-- Parameters:
--      i_include_disabled       - flag indicating if to include disabled schemas too
--
-- Returns:
--      schema_name             - name of the schema
--      schema_latest_version   - the latest version of the schema
--
-------------------------------------------------------------------------------
DECLARE
BEGIN
    RETURN QUERY
    SELECT dsh.schema_name, dsh.schema_latest_version
    FROM dataset_schema.heads dsh
    WHERE i_include_disabled OR dsh.disabled_when IS NULL
    ORDER BY schema_name; --TODO Include order by?
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION dataset_schema.list_schemas(BOOLEAN) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION dataset_schema.list_schemas(BOOLEAN) TO menas;
