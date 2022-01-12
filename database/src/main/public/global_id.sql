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


--DROP SEQUENCE IF  EXISTS public.global_id_seq

-- SERVER should be a unique number within your deployment between 0 and 9222
CREATE SEQUENCE IF NOT EXISTS public.global_id_seq
    INCREMENT 1
    START [SERVER_ID]*1000000000000000+1
    MINVALUE [SERVER_ID]*1000000000000000+1
    MAXVALUE [SERVER_ID] + 1)*1000000000000000
    CACHE 10;

CREATE OR REPLACE FUNCTION public.global_id() RETURNS BIGINT AS
$$
-------------------------------------------------------------------------------
--
-- Function: public.global_id(0)
--      Generates a unique ID
--
-- Returns:
--      - The next ID to use
--
-------------------------------------------------------------------------------
DECLARE
BEGIN
    RETURN nextval('global_id_seq');
END;
$$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION public.global_id() TO PUBLIC;
