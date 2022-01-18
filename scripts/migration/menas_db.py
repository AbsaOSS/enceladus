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

from pymongo.database import Database
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from constants import *
from typing import List


class MenasDbError(Exception):
    """Menas-specific db error"""
    pass


class MenasDbVersionError(MenasDbError):
    """Error related to Menas db-version (set in collection db_version)"""
    pass


class MenasDbCollectionError(MenasDbError):
    """Error related to Menas-specific collections in the mongo DB"""
    pass


class MenasDb(object):
    def __init__(self, mongodb: Database, alias: str = None, verbose: bool = False):
        self.mongodb = mongodb
        self.alias = alias
        self.hint = f" ({self.alias})" if self.alias else ""  # " (alias)" or empty to append to stuff
        self.verbose = verbose

    @staticmethod
    def from_connection_string(connection_string: str, database_name: str, alias: str = None,
                               verbose: bool = False):
        db = MenasDb.get_database(connection_string, database_name)
        return MenasDb(db, alias, verbose)

    def check_db_version(self) -> None:
        """
        Checks the db for having collection #DB_VERSION_COLLECTION with a record having version=1
        """
        if DB_VERSION_COLLECTION in set(self.mongodb.list_collection_names()):
            #  check version record
            collection = self.mongodb[DB_VERSION_COLLECTION]

            version_record = collection.find_one()
            if self.verbose:
                print(f"Version record retrieval attempt: {version_record}")
            if version_record:
                if version_record["version"] != 1:
                    raise MenasDbVersionError(f"This script requires {DB_VERSION_COLLECTION}{self.hint} record version=1, " +
                                              f"but found: {version_record}")  # deliberately the whole record

            else:
                raise MenasDbVersionError(f"No version record found in {DB_VERSION_COLLECTION}{self.hint}!")
        else:
            raise MenasDbVersionError(f"DB {self.mongodb.name}{self.hint} does not contain collection {DB_VERSION_COLLECTION}!")

    def create_db_version(self, silent: bool = True) -> None:
        """
        Attempts to set collection #DB_VERSION_COLLECTION with a record having version=1
        :param silent: prints information alongside
        """
        def sprint(string: str) -> None:
            if not silent:
                print(string)

        collection = self.mongodb[DB_VERSION_COLLECTION]
        version_record = collection.find_one()
        if self.verbose:
            sprint(f"  Version record retrieval attempt: {version_record}")

        # either version=1 record exists, or create new
        if version_record:
            if version_record["version"] != 1:
                # failing on incompatible version, because that may corrupt data
                raise MenasDbVersionError(f"Existing incompatible version record has been found in"
                                          f" {DB_VERSION_COLLECTION}{self.hint}: {version_record}")
            else:
                sprint("  Existing db_version version=1 record found.")
        else:
            insert_result = collection.insert_one({"version": 1})
            sprint(f"  Created version=1 record with id {insert_result.inserted_id}")
        sprint("")

    def check_menas_collections_exist(self) -> None:
        """
        Ensures given database contains expected collections to migrate (see #MIGRATING_COLLECTIONS).
        Raises an exception with description if expected collections are not found in the db.
        """
        def ensure_collections_exist(collection_names: List[str]) -> None:
            existing_collections = self.mongodb.list_collection_names()
            for collection_name in collection_names:
                if not(collection_name in existing_collections):
                    raise MenasDbCollectionError(f"Collection '{collection_name}' not found in database "
                                                 f"'{self.mongodb.name}'{self.hint}.")

        return ensure_collections_exist(MIGRATING_COLLECTIONS)

    def get_distinct_ds_names_from_ds_names(self, ds_names: List[str], not_locked_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(ds_names, DATASET_COLLECTION, not_locked_only)

    def get_distinct_schema_names_from_schema_names(self, schema_names: List[str],
                                                    not_locked_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(schema_names, SCHEMA_COLLECTION, not_locked_only)

    def get_distinct_entities_ids(self, entity_names: List[str], collection_name: str, not_locked_only: bool,
                                  entity_name_field: str = "name", distinct_field: object = "name") -> List[str]:
        """ General way to retrieve distinct entity field values (names, ids, ...) from non-locked entities """
        collection = self.mongodb[collection_name]
        locked_filter = NOT_LOCKED_MONGO_FILTER if not_locked_only else EMPTY_MONGO_FILTER

        entities = collection.distinct(
            distinct_field,  # field to distinct on
            {"$and": [
                {entity_name_field: {"$in": entity_names}},  # filter on name (ds/mt)
                locked_filter
            ]}
        )
        return entities  # list of distinct names (in a single document)

    def get_distinct_mapping_tables_from_ds_names(self, ds_names: List[str], not_locked_only: bool) -> List[str]:
        ds_collection = self.mongodb[DATASET_COLLECTION]
        locked_filter = NOT_LOCKED_MONGO_FILTER if not_locked_only else EMPTY_MONGO_FILTER

        mapping_table_names = ds_collection.aggregate([
            {"$match": {"$and": [  # selection based on:
                {"name": {"$in": ds_names}},  # dataset name
                {"conformance": {"$elemMatch": {"_t": "MappingConformanceRule"}}},  # having some MCRs
                locked_filter
            ]}},
            {"$unwind": "$conformance"},  # explodes each doc into multiple - each having single conformance rule
            {"$match": {"conformance._t": "MappingConformanceRule"}},  # filtering only MCRs, other CR are irrelevant
            {"$group": {
                "_id": "notNeededButRequired",
                "mts": {"$addToSet": "$conformance.mappingTable"}
            }}  # grouping on fixed id (essentially distinct) and adding all MTs to a set
        ])  # single doc with { _id: ... , "mts" : [mt1, mt2, ...]}

        # if no MCRs are present, the result may be empty
        mapping_table_names_list = list(mapping_table_names)  # cursor behaves one-iteration only.
        if not list(mapping_table_names_list):
            return []

        extracted_list = mapping_table_names_list[0]['mts']
        return extracted_list

    def assemble_notlocked_runs_from_ds_names(self, ds_names: List[str]) -> List[str]:
        return self.get_distinct_entities_ids(ds_names, RUN_COLLECTION, entity_name_field="dataset",
                                              distinct_field="uniqueId", not_locked_only=True)

    def assemble_schemas_from_ds_names(self, ds_names: List[str], not_locked_only: bool) -> List[str]:
        return self._assemble_schemas(ds_names, DATASET_COLLECTION, "schemaName", not_locked_only)

    def assemble_schemas_from_mt_names(self, mt_names: List[str], not_locked_only: bool) -> List[str]:
        return self._assemble_schemas(mt_names, MAPPING_TABLE_COLLECTION, "schemaName", not_locked_only)

    def _assemble_schemas(self, entity_names: List[str], collection_name: str,
                          distinct_field: str, not_locked_only: bool) -> List[str]:
        """ Common processing method for `assemble_schemas_from_ds_names` and `assemble_schemas_from_mt_names` """
        # schema names from locked+notlocked (datasets/mts) (the schemas themselves may or may not be locked):
        schema_names = self.get_distinct_entities_ids(entity_names, collection_name, distinct_field=distinct_field,
                                                      not_locked_only=False)
        # check schema collection which of these schemas are actually (not) locked:
        return self.get_distinct_schema_names_from_schema_names(schema_names, not_locked_only)

    def assemble_mapping_tables_from_mt_names(self, mt_names: List[str], not_locked_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(mt_names, MAPPING_TABLE_COLLECTION, not_locked_only)

    def assemble_notlocked_mapping_tables_from_ds_names(self, ds_names: List[str]) -> List[str]:
        # mt names from locked+notlocked datasets (the mts themselves may or may not be locked)
        mt_names_from_ds_names = self.get_distinct_mapping_tables_from_ds_names(ds_names, not_locked_only=False)
        # ids for not locked mapping tables
        return self.get_distinct_entities_ids(mt_names_from_ds_names, MAPPING_TABLE_COLLECTION, not_locked_only=True)

    def assemble_notlocked_attachments_from_schema_names(self, schema_names: List[str]) -> List[str]:
        return self.get_distinct_entities_ids(schema_names, ATTACHMENT_COLLECTION, entity_name_field="refName",
                                              distinct_field="refName", not_locked_only=True)

    @staticmethod
    def get_database(conn_str: str, db_name: str) -> Database:
        """
        Gets db handle
        :param db_name: string db name
        :param conn_str: connection string, e.g. mongodb://username1:password213@my.domain.ext/adminOrAnotherDb
        :return: MongoDB handle
        """
        client = MongoClient(conn_str)
        majority_write_concern = WriteConcern(w="majority")
        majority_read_concern = ReadConcern(level="majority")

        return Database(client, db_name, write_concern=majority_write_concern, read_concern=majority_read_concern)
