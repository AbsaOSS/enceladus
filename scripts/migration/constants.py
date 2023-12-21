#!/usr/bin/env python3

# Copyright 2022 ABSA Group Limited
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
import pymongo

DEFAULT_VERBOSE = False
DEFAULT_DRYRUN = False
DEFAULT_DB_NAME = "menas"  # both for source and target dbs

# Other constants
LOCKING_USER = "migration"

MIGRATIONFREE_MONGO_FILTER = {"migrationHash": {"$exists": False}}
EMPTY_MONGO_FILTER = {}

SCHEMA_COLLECTION = "schema_v1"
DATASET_COLLECTION = "dataset_v1"
MAPPING_TABLE_COLLECTION = "mapping_table_v1"
RUN_COLLECTION = "run_v1"
ATTACHMENT_COLLECTION = "attachment_v1"
PROPERTY_DEF_COLLECTION = "propertydef_v1"
LANDING_PAGE_STATS_COLLECTION = "landing_page_statistics_v1"

DB_VERSION_COLLECTION = "db_version"

# collections that contain data to migrate using the migrate_menas.py
DATA_MIGRATING_COLLECTIONS = [SCHEMA_COLLECTION, DATASET_COLLECTION, MAPPING_TABLE_COLLECTION, RUN_COLLECTION,
                              ATTACHMENT_COLLECTION, PROPERTY_DEF_COLLECTION]

# all collections that are used in the scripts - this is used by initialize_menas.py
ALL_COLLECTIONS = DATA_MIGRATING_COLLECTIONS + [LANDING_PAGE_STATS_COLLECTION]

# field names for the INDICES structure as well as keys used by result of <collection>.index_information()
INDICES_FIELD_KEY = "key"
INDICES_FIELD_UNIQUE = "unique"
INDICES_FIELD_SPARSE = "sparse"

# Indices to be by default set on collections (with the unique option)
_MOST_COMMON_INDEX_LIST = [
    {
        INDICES_FIELD_KEY: [("name", pymongo.ASCENDING), ("version", pymongo.ASCENDING)],
        INDICES_FIELD_UNIQUE: True
    }
]
INDICES = {
    SCHEMA_COLLECTION: _MOST_COMMON_INDEX_LIST,
    DATASET_COLLECTION: _MOST_COMMON_INDEX_LIST,
    MAPPING_TABLE_COLLECTION: _MOST_COMMON_INDEX_LIST,
    RUN_COLLECTION: [
        {
            INDICES_FIELD_KEY: [
                ("dataset", pymongo.ASCENDING), ("datasetVersion", pymongo.ASCENDING), ("runId", pymongo.ASCENDING)
            ],
            INDICES_FIELD_UNIQUE: True
        },
        {
            INDICES_FIELD_KEY: [("uniqueId", pymongo.ASCENDING)],
            INDICES_FIELD_UNIQUE: True,
            INDICES_FIELD_SPARSE: True
        }
    ],
    ATTACHMENT_COLLECTION: [
        {INDICES_FIELD_KEY: [("refName", pymongo.ASCENDING), ("refVersion", pymongo.ASCENDING)]}
    ]
}
