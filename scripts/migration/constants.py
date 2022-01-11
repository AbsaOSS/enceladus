#!/usr/bin/env python3

# Copyright 2018 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Defaults
DEFAULT_VERBOSE = False
DEFAULT_DRYRUN = False
DEFAULT_DB_NAME = "menas"  # both for source and target dbs

# Other constants
LOCKING_USER = "migration"

NOT_LOCKED_MONGO_FILTER = {"$or": [
    {"locked": False},  # is not locked, or
    {"locked": {"$exists": False}}  # or: there is no locking info at all
]}
EMPTY_MONGO_FILTER = {}

SCHEMA_COLLECTION = "schema_v1"
DATASET_COLLECTION = "dataset_v1"
MAPPING_TABLE_COLLECTION = "mapping_table_v1"
RUN_COLLECTION = "run_v1"
ATTACHMENT_COLLECTION = "attachment_v1"

MIGRATING_COLLECTIONS = [SCHEMA_COLLECTION, DATASET_COLLECTION, MAPPING_TABLE_COLLECTION, RUN_COLLECTION,
                         ATTACHMENT_COLLECTION]
