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

CREATE OR REPLACE FUNCTION mapping_table.list(
    IN  i_include_disabled      BOOLEAN DEFAULT FALSE,
    OUT entity_name             TEXT,
    OUT entity_latest_version   INTEGER,
    OUT locked                  BOOLEAN,
    OUT disabled                BOOLEAN
) RETURNS SETOF record AS
$$
-------------------------------------------------------------------------------
--
-- Function: jobs_configuration.list(1)
--      Returns a list of mapping tables with their latest versions
--
-- Parameters:
--      i_include_disabled      - flag indicating if to include disabled mapping tables too
--
-- Returns:
--      entity_name             - name of the mapping table
--      entity_latest_version   - the latest version of the mapping table
--      locked                  - signals if the mapping table is locked or not
--      disabled                - signals if the mapping table is disabled or not
--
-------------------------------------------------------------------------------
DECLARE
BEGIN
    RETURN QUERY
    SELECT E.entity_name, E.entity_latest_version, E.locked_at IS NOT NULL, E.disabled_at IS NOT NULL
    FROM mapping_table.entities E
    WHERE i_include_disabled OR E.disabled_at IS NULL
    ORDER BY E.entity_name; --TODO Include order by?
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

ALTER FUNCTION mapping_table.list(BOOLEAN) OWNER TO enceladus;
GRANT EXECUTE ON FUNCTION mapping_table.list(BOOLEAN) TO menas;
