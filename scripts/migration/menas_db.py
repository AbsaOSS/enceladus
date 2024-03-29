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

from pymongo.database import Database
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from constants import *
from typing import List


class MenasDbError(Exception):
    """Menas-specific db error"""
    pass


class MenasNoDbVersionRecordError(MenasDbError):
    """Error meaning that there is no Menas db_version record (missing or empty collection)"""


class MenasDbVersionError(MenasDbError):
    """Error of Menas db-version record being present, but unsuitable (version!=1)"""

    def __init__(self, message, found_version: int = None):
        assert found_version != 1, "MenasDbVersionError cannot hold version=1, this would not result in an error"
        self.found_version = found_version
        self.message = message
        super().__init__(self.message)


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
            if version_record is not None:
                version_found = version_record.get("version", None)
                if version_found is None:
                    raise MenasDbVersionError(f"This script requires {DB_VERSION_COLLECTION}{self.hint} record version=1, " +
                                              f"but found record: {version_record}")  # deliberately the whole record
                else:
                    if version_found != 1:
                        raise MenasDbVersionError(f"This script requires {DB_VERSION_COLLECTION}{self.hint} record version=1, " +
                                                  f"but found version: {version_found}", found_version=version_found)
            else:
                raise MenasNoDbVersionRecordError(f"No version record found in {DB_VERSION_COLLECTION}{self.hint}!")
        else:
            raise MenasNoDbVersionRecordError(f"DB {self.mongodb.name}{self.hint} does not contain collection {DB_VERSION_COLLECTION}!")

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

        # deliberately only checking migrating collections, not all (landing_page_statistics may not be present)
        return ensure_collections_exist(DATA_MIGRATING_COLLECTIONS)

    def get_distinct_ds_names_from_ds_names(self, ds_names: List[str], migration_free_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(ds_names, DATASET_COLLECTION, migration_free_only)

    def get_distinct_mt_names_from_mt_names(self, mt_names: List[str], migration_free_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(mt_names, MAPPING_TABLE_COLLECTION, migration_free_only)

    def get_distinct_schema_names_from_schema_names(self, schema_names: List[str],
                                                    migration_free_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(schema_names, SCHEMA_COLLECTION, migration_free_only)

    def get_all_distinct_entities_ids(self, collection_name: str, migration_free_only: bool,
                                      distinct_field: str = "name") -> List[str]:
        """Special case of get_distinct_entities_ids without supplying list of entity names - yields all"""
        return self.get_distinct_entities_ids(entity_names=None, collection_name=collection_name,
                                              migration_free_only=migration_free_only, entity_name_field=None,
                                              distinct_field=distinct_field)

    def get_distinct_entities_ids(self, entity_names: List[str], collection_name: str, migration_free_only: bool,
                                  entity_name_field: str = "name", distinct_field: str = "name") -> List[str]:
        """ General way to retrieve distinct entity field values (names, ids, ...) entities (optionally migration-free) """
        collection = self.mongodb[collection_name]
        migrating_filter = MIGRATIONFREE_MONGO_FILTER if migration_free_only else EMPTY_MONGO_FILTER
        name_filter = {entity_name_field: {"$in": entity_names}} if entity_names is not None else {}

        entities = collection.distinct(
            distinct_field,  # field to distinct on
            {"$and": [
                name_filter,
                migrating_filter
            ]}
        )
        return entities  # list of distinct names (in a single document)

    def get_distinct_mapping_tables_from_ds_names(self, ds_names: List[str], migration_free_only: bool) -> List[str]:
        ds_collection = self.mongodb[DATASET_COLLECTION]
        migrating_filter = MIGRATIONFREE_MONGO_FILTER if migration_free_only else EMPTY_MONGO_FILTER

        mapping_table_names = ds_collection.aggregate([
            {"$match": {"$and": [  # selection based on:
                {"name": {"$in": ds_names}},  # dataset name
                {"conformance": {"$elemMatch": {"_t": "MappingConformanceRule"}}},  # having some MCRs
                migrating_filter
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

    def get_property_names_from_ds_names(self, ds_names: List[str]) -> List[str]:
        ds_collection = self.mongodb[DATASET_COLLECTION]
        props_agg_result = ds_collection.aggregate([
            {"$match": {"$and": [  # selection based on:
                {"name": {"$in": ds_names}},  # dataset name
                {"properties": {"$ne": None}},  # having some properties
                {"properties": {"$ne": {}}}
                # no "migration_filter" - we always want property names from all matching datasets
            ]}},
            {"$project": {
                "props": {"$objectToArray": "$properties"}   # turns object into map with "k" and "v" fields.
            }},
            {"$unwind": "$props"},  # explodes each doc into multiple - each having prop key/val
            {"$group": {
                "_id": "notNeededButRequired",
                "propkeys": {"$addToSet": "$props.k"}
            }}
        ])  # single doc with { _id: ... , "propkeys" : [propKey1, propKey2, ...]}

        # if no props are present, the result may be empty
        propdef_names_list = list(props_agg_result)  # cursor behaves one-iteration only.
        if not list(propdef_names_list):
            return []

        extracted_list = propdef_names_list[0]['propkeys']
        return extracted_list

    def assemble_migration_free_runs_from_ds_names(self, ds_names: List[str]) -> List[str]:
        return self.get_distinct_entities_ids(ds_names, RUN_COLLECTION, entity_name_field="dataset",
                                              distinct_field="uniqueId", migration_free_only=True)

    def assemble_schemas_from_ds_names(self, ds_names: List[str], migration_free_only: bool) -> List[str]:
        return self._assemble_schemas(ds_names, DATASET_COLLECTION, "schemaName", migration_free_only)

    def assemble_schemas_from_mt_names(self, mt_names: List[str], migration_free_only: bool) -> List[str]:
        return self._assemble_schemas(mt_names, MAPPING_TABLE_COLLECTION, "schemaName", migration_free_only)

    def _assemble_schemas(self, entity_names: List[str], collection_name: str,
                          distinct_field: str, migration_free_only: bool) -> List[str]:
        """ Common processing method for `assemble_schemas_from_ds_names` and `assemble_schemas_from_mt_names` """
        # schema names from migration-bearing + migration-free (datasets/mts) (the schemas themselves may or may not be migrated):
        schema_names = self.get_distinct_entities_ids(entity_names, collection_name, distinct_field=distinct_field,
                                                      migration_free_only=False)
        # check schema collection which of these schemas are actually migration-free:
        return self.get_distinct_schema_names_from_schema_names(schema_names, migration_free_only)

    def assemble_mapping_tables_from_mt_names(self, mt_names: List[str], migration_free_only: bool) -> List[str]:
        return self.get_distinct_entities_ids(mt_names, MAPPING_TABLE_COLLECTION, migration_free_only)

    def assemble_migration_free_mapping_tables_from_ds_names(self, ds_names: List[str]) -> List[str]:
        # mt names from migration-bearing + migration-free datasets (the mts themselves may or may not be migration-bearing)
        mt_names_from_ds_names = self.get_distinct_mapping_tables_from_ds_names(ds_names, migration_free_only=False)
        # ids for migration-free mapping tables
        return self.get_distinct_entities_ids(mt_names_from_ds_names, MAPPING_TABLE_COLLECTION, migration_free_only=True)

    def assemble_migration_free_attachments_from_schema_names(self, schema_names: List[str]) -> List[str]:
        return self.get_distinct_entities_ids(schema_names, ATTACHMENT_COLLECTION, entity_name_field="refName",
                                              distinct_field="refName", migration_free_only=True)

    def assemble_migration_free_propdefs_from_prop_names(self, prop_names: List[str]) -> List[str]:
        return self.get_distinct_entities_ids(prop_names, PROPERTY_DEF_COLLECTION, migration_free_only=True)

    def assemble_all_propdefs(self, migration_free_only=True) -> List[str]:
        return self.get_all_distinct_entities_ids(PROPERTY_DEF_COLLECTION, migration_free_only=migration_free_only)

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
