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

import argparse
import secrets  # migration hash generation

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.errors import DuplicateKeyError
from typing import List
from datetime import datetime, timezone

from constants import *

# python package needed are denoted in requirements.txt, so to fix missing dependencies, just run
# pip install -r requirements.txt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='migrate_menas',
        description='Menas MongoDB migration script.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # prints default values, too, on help (-h)
    )

    parser.add_argument('-n', '--dryrun', action='store_true', default=DEFAULT_DRYRUN,
                        help="if specified, skip the actual synchronization, just print what would be copied over.")
    parser.add_argument('-v', '--verbose', action="store_true", default=DEFAULT_VERBOSE,
                        help="prints extra information while running.")

    parser.add_argument('source', metavar="SOURCE", help="connection string for source MongoDB")
    parser.add_argument('target', metavar="TARGET", help="connection string for target MongoDB")

    parser.add_argument('-t', '--target-database', dest="targetdb", default=DEFAULT_DB_NAME,
                        help="Name of db on target to migrate to.")
    parser.add_argument('-s', '--source-database', dest="sourcedb", default=DEFAULT_DB_NAME,
                        help="Name of db on source to migrate from.")

    input_options_group = parser.add_mutually_exclusive_group(required=True)
    input_options_group.add_argument('-d', '--datasets', dest='datasets', metavar="DATASET_NAME", default=[],
                                     nargs="+", help='list datasets to migrate')
    input_options_group.add_argument('-m', '--mapping-tables', dest="mtables", metavar="MTABLE_NAME", default=[],
                                     nargs="+", help='list datasets to migrate')

    return parser.parse_args()


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


def get_distinct_ds_names_from_ds_names(db: Database, ds_names: List[str], not_locked_only: bool) -> List[str]:
    return get_distinct_entities_ids(db, ds_names, DATASET_COLLECTION, not_locked_only)


def get_distinct_schema_names_from_schema_names(db: Database, schema_names: List[str],
                                                not_locked_only: bool) -> List[str]:
    return get_distinct_entities_ids(db, schema_names, SCHEMA_COLLECTION, not_locked_only)


def get_distinct_entities_ids(db: Database, entity_names: List[str], collection_name: str, not_locked_only: bool,
                              entity_name_field: str = "name", distinct_field: object = "name") -> List[str]:
    """ General way to retrieve distinct entity field values (names, ids, ...) from non-locked entities """
    collection = db[collection_name]
    locked_filter = NOT_LOCKED_MONGO_FILTER if not_locked_only else EMPTY_MONGO_FILTER

    entities = collection.distinct(
        distinct_field,  # field to distinct on
        {"$and": [
            {entity_name_field: {"$in": entity_names}},  # filter on name (ds/mt)
            locked_filter
        ]}
    )
    return entities  # list of distinct names (in a single document)


def get_distinct_mapping_tables_from_ds_names(db: Database, ds_names: List[str], not_locked_only: bool) -> List[str]:
    ds_collection = db[DATASET_COLLECTION]
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


def assemble_notlocked_runs_from_ds_names(db: Database, ds_names: List[str]) -> List[str]:
    return get_distinct_entities_ids(db, ds_names, RUN_COLLECTION, entity_name_field="dataset", distinct_field="uniqueId",
                                     not_locked_only=True)


def assemble_schemas_from_ds_names(db: Database, ds_names: List[str], not_locked_only: bool) -> List[str]:
    return assemble_schemas(db, ds_names, DATASET_COLLECTION, "schemaName", not_locked_only)


def assemble_schemas_from_mt_names(db: Database, mt_names: List[str], not_locked_only: bool) -> List[str]:
    return assemble_schemas(db, mt_names, MAPPING_TABLE_COLLECTION, "schemaName", not_locked_only)


def assemble_schemas(db: Database, entity_names: List[str], collection_name: str,
                     distinct_field: str, not_locked_only: bool) -> List[str]:
    """ Common processing method for `assemble_schemas_from_ds_names` and `assemble_schemas_from_mt_names` """
    # schema names from locked+notlocked (datasets/mts) (the schemas themselves may or may not be locked):
    schema_names = get_distinct_entities_ids(db, entity_names, collection_name, distinct_field=distinct_field,
                                             not_locked_only=False)
    # check schema collection which of these schemas are actually (not) locked:
    return get_distinct_schema_names_from_schema_names(db, schema_names, not_locked_only)


def assemble_mapping_tables_from_mt_names(db: Database, mt_names: List[str],
                                          not_locked_only: bool) -> List[str]:
    return get_distinct_entities_ids(db, mt_names, MAPPING_TABLE_COLLECTION, not_locked_only)


def assemble_notlocked_mapping_tables_from_ds_names(db: Database, ds_names: List[str]) -> List[str]:
    # mt names from locked+notlocked datasets (the mts themselves may or may not be locked)
    mt_names_from_ds_names = get_distinct_mapping_tables_from_ds_names(db, ds_names, not_locked_only=False)
    # ids for not locked mapping tables
    return get_distinct_entities_ids(db, mt_names_from_ds_names, MAPPING_TABLE_COLLECTION, not_locked_only=True)


def assemble_notlocked_attachments_from_schema_names(db: Database, schema_names: List[str]) -> List[str]:
    return get_distinct_entities_ids(db, schema_names, ATTACHMENT_COLLECTION, entity_name_field="refName",
                                     distinct_field="refName", not_locked_only=True)


def get_date_locked_structure(dt: datetime) -> dict:
    return {
        "dateTime": {
            "date": {"year": dt.year, "month": dt.month, "day": dt.day},
            "time": {"hour": dt.hour, "minute": dt.minute, "second": dt.second,"nano": dt.microsecond * 1000}
        },
        "offset": 0,
        "zone": "UTC"
    }


def migrate_entities(source_db: Database, target_db: Database, collection_name: str, entity_names_list: List[str],
                     describe_fn, entity_name: str = "entity", name_field: str = "name") -> None:
    if not entity_names_list:
        print("No {}s to migrate in {}, skipping.".format(entity_name, collection_name))
        return

    print("Migration of collection {} started".format(collection_name))
    dataset_collection = source_db[collection_name]

    # mark as locked first in source
    locking_update_result = dataset_collection.update_many(
        {"$and": [
            {name_field: {"$in": entity_names_list}},  # dataset/schema/mt name or run uniqueId
            NOT_LOCKED_MONGO_FILTER
        ]},
        {"$set": {
            "migrationHash": migration_hash,  # script-global var
            "locked": True,
            "userLocked": LOCKING_USER,
            "dateLocked": get_date_locked_structure(utc_now) # script-global var
        }}
    )

    locked_count = locking_update_result.modified_count
    if locking_update_result.acknowledged:
        if verbose:
            print("Successfully locked {}s: {}. Migrating ... ".format(entity_name, locked_count))
    else:
        raise Exception("Locking unsuccessful: {}, matched={}, modified={}"
                        .format(locking_update_result.acknowledged, locking_update_result.matched_count, locking_update_result.modified_count))

    # This relies on the locking-update being complete on mongo-cluster, thus using majority r/w concerns.
    # https://docs.mongodb.com/manual/core/causal-consistency-read-write-concerns/#std-label-causal-rc-majority-wc-majority
    docs = dataset_collection.find(
        {"$and": [
            {name_field: {"$in": entity_names_list}},  # dataset name
            {"migrationHash": migration_hash},  # belongs to this migration # script-global var
            {"locked": True}  # is successfully locked (previous step)
        ]}
    )

    # migrate locked entities from source to target
    target_dataset_collection = target_db[collection_name]
    migrated_count = 0
    dupe_kvs = []  # keeping the dupe-ids here
    for item in docs:
        # item preview
        if verbose:
            print("Migrating {}: {}.".format(entity_name, describe_fn(item)))

        # the original is locked (+ has user/datetime info) + migration #, target carries has migration #.
        del item["locked"]
        del item["userLocked"]
        del item["dateLocked"]

        try:
            target_dataset_collection.insert_one(item)
        except DuplicateKeyError as e:
            dupe_kv = e.details['keyValue']
            dupe_kvs.append(dupe_kv)
            print("Warning: The {} IDed by {} already found on target, skipping it.".format(entity_name, dupe_kv))
        else:
            migrated_count += 1

    dupe_count = len(dupe_kvs)
    # mark migrated/existing duplicates as migrated or report problems
    if locked_count == migrated_count + dupe_count:
        migration_tagging_update_result = dataset_collection.update_many(
            {"$and": [
                {name_field: {"$in": entity_names_list}},  # dataset/schema/mt name or run uniqueId
                {"migrationHash": migration_hash}, # script-global var
                {"locked": True}
            ]},
            {"$set": {
                "migrated": True
            }}
        )
        migration_tagging_count = migration_tagging_update_result.modified_count
        if migration_tagging_update_result.acknowledged and migration_tagging_count == locked_count:
            if verbose:
                print("Successfully marked {}s as migrated: {}. ".format(entity_name, migration_tagging_count))
        else:
            raise Exception("Migration tagging unsuccessful: {}, matched={}, modified={}"
                            .format(migration_tagging_update_result.acknowledged,
                                    migration_tagging_update_result.matched_count,
                                    locking_update_result.modified_count))

    else:
        raise Exception("Locked {} {}s, but managed to migrate only {} of them (dupe skipped: {})!"
                        .format(locked_count, entity_name, migrated_count, dupe_count))

    if dupe_count == 0:
        print("Migration of collection {} finished, migrated {} {}s.\n"
              .format(collection_name, migrated_count, entity_name))
    else:
        print("Migration of collection {} finished, migrated {} {}s,".format(collection_name, migrated_count, entity_name) +
              " skipped {} {}s (duplicate already existed on target).\n".format(dupe_count, entity_name))


def describe_default_entity(item: dict) -> str:
    """
    Aux method to describe dataset/schema/mapping-table object - relying on fields 'name' and 'version' being present
    :param item: object to describe
    :return: formatted description string
    """
    return "{} v{}".format(item["name"], item["version"])


def describe_run_entity(item: dict) -> str:
    """
    Aux method to describe run object - relying on fields 'dataset', 'datasetVersion', and 'uniqueId' being present
    :param item: object to describe
    :return: formatted description string
    """
    return "for {} v{} - run {} (uniqueId {})".format(item["dataset"], item["datasetVersion"], item["runId"],
                                                      item["uniqueId"])


def describe_attachment_entity(item: dict) -> str:
    """
    Aux method to describe attachment object - relying on fields 'refCollection', 'refName', and 'refVersion' being
    present
    :param item: object to describe
    :return: formatted description string
    """
    return "attachment for {} {} v{}".format(item["refCollection"], item["refName"], item["refVersion"])


def ensure_menas_collections_exist(db: Database, alias: str = "") -> None:
    """
    Ensures given database contains expected collections to migrate (see #MIGRATING_COLLECTIONS).
    Raises an exception with description if expected collections are not found in the db.
    :param db: database to check
    :param alias: name for db to print, e.g. "source"
    """
    def ensure_collections_exist(collection_names: List[str]) -> None:
        existing_collections = db.list_collection_names()
        for collection_name in collection_names:
            if not(collection_name in existing_collections):
                hint = " ({})".format(alias) if alias else ""
                raise Exception("Collection '{}' not found in database '{}'{}. ".format(collection_name, db.name, hint)
                                + "Are you sure your setup is correct?")

    return ensure_collections_exist(MIGRATING_COLLECTIONS)


# TODO when we have a menas db-initialization script (pre-migration), let's merge this with
#  `ensure_menas_collections_exist` - to assess basic validity of source & target alike - Issue #2013
def ensure_db_version(db: Database, alias: str = "") -> None:
    """
    Checks the db for having collection #DB_VERSION_COLLECTION with a record having version=1
    :param db: database to check
    :param alias: name for db to print, e.g. "source"
    """
    hint = f" ({alias})" if alias else ""

    if DB_VERSION_COLLECTION in set(db.list_collection_names()):
        #  check version record
        collection = db[DB_VERSION_COLLECTION]

        version_record = collection.find_one()
        print(f"version record: {version_record}")
        if version_record:
            if version_record["version"] != 1:
                raise Exception(f"This script requires {DB_VERSION_COLLECTION}{hint} record version=1, " +
                                f"but found: {version_record}")  # deliberately the whole record

        else:
            raise Exception(f"No version record found in {DB_VERSION_COLLECTION}{hint}!")
    else:
        raise Exception(f"DB {db.name}{hint} does not contain collection {DB_VERSION_COLLECTION}!")


def migrate_collections_by_ds_names(source_db: Database, target_db: Database,
                                    supplied_ds_names: List[str],
                                    dryrun: bool) -> None:

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names = get_distinct_ds_names_from_ds_names(source_db, supplied_ds_names, not_locked_only=False)
    notlocked_ds_names = get_distinct_ds_names_from_ds_names(source_db, supplied_ds_names, not_locked_only=True)
    print('Dataset names to migrate: {}'.format(notlocked_ds_names))

    ds_schema_names = assemble_schemas_from_ds_names(source_db, ds_names, not_locked_only=False)
    notlocked_ds_schema_names = assemble_schemas_from_ds_names(source_db, ds_names, not_locked_only=True)
    print('DS schemas to migrate: {}'.format(notlocked_ds_schema_names))

    notlocked_mapping_table_names = assemble_notlocked_mapping_tables_from_ds_names(source_db, ds_names)
    mapping_table_names = get_distinct_mapping_tables_from_ds_names(source_db, ds_names, not_locked_only=False)
    print('MTs to migrate: {}'.format(notlocked_mapping_table_names))

    mt_schema_names = assemble_schemas_from_mt_names(source_db, mapping_table_names, not_locked_only=False)
    # final MT schemas must be retrieved from locked MTs, too, not just notlocked_mapping_table_names
    notlocked_mt_schema_names = assemble_schemas_from_mt_names(source_db, mapping_table_names, not_locked_only=True)
    print('MT schemas to migrate: {}'.format(notlocked_mt_schema_names))

    run_unique_ids = assemble_notlocked_runs_from_ds_names(source_db, ds_names)
    print('Runs to migrate: {}'.format(run_unique_ids))

    all_notlocked_schemas = list(set.union(set(notlocked_ds_schema_names), set(notlocked_mt_schema_names)))
    if verbose:
        print('All schemas (DS & MT) to migrate: {}'.format(all_notlocked_schemas))

    # attachments from locked schemas, too:
    schemas_names_for_attachments = list(set.union(set(ds_schema_names), set(mt_schema_names)))  # locked+unlocked
    notlocked_attachment_names = assemble_notlocked_attachments_from_schema_names(source_db, schemas_names_for_attachments)
    print('Attachments of schemas to migrate: {}'.format(notlocked_attachment_names))

    if not dryrun:
        print("\n")
        migrate_entities(source_db, target_db, SCHEMA_COLLECTION, all_notlocked_schemas, describe_default_entity, entity_name="schema")
        migrate_entities(source_db, target_db, DATASET_COLLECTION, notlocked_ds_names, describe_default_entity, entity_name="dataset")
        migrate_entities(source_db, target_db, MAPPING_TABLE_COLLECTION, notlocked_mapping_table_names,
                         describe_default_entity, entity_name="mapping table")
        migrate_entities(source_db, target_db, RUN_COLLECTION, run_unique_ids, describe_run_entity, entity_name="run", name_field="uniqueId")
        migrate_entities(source_db, target_db, ATTACHMENT_COLLECTION, notlocked_attachment_names,
                         describe_attachment_entity, entity_name="attachment", name_field="refName")
    else:
        print("*** Dryrun selected, no actual migration will take place.")


def migrate_collections_by_mt_names(source_db: Database, target_db: Database,
                                    supplied_mt_names: List[str],
                                    dryrun: bool) -> None:
    if verbose:
        print("MT names given: {}".format(supplied_mt_names))

    notlocked_mapping_table_names = assemble_mapping_tables_from_mt_names(source_db, supplied_mt_names, not_locked_only=True)
    print('MTs to migrate: {}'.format(notlocked_mapping_table_names))

    notlocked_mt_schema_names = assemble_schemas_from_mt_names(source_db, supplied_mt_names, not_locked_only=True)
    print('MT schemas to migrate: {}'.format(notlocked_mt_schema_names))

    mt_schema_names = assemble_schemas_from_mt_names(source_db, supplied_mt_names, not_locked_only=False)
    notlocked_attachment_names = assemble_notlocked_attachments_from_schema_names(source_db, mt_schema_names)
    print('Attachments of schemas to migrate: {}'.format(notlocked_attachment_names))

    if not dryrun:
        print("\n")
        migrate_entities(source_db, target_db, SCHEMA_COLLECTION, notlocked_mt_schema_names, describe_default_entity, entity_name="schema")
        migrate_entities(source_db, target_db, MAPPING_TABLE_COLLECTION, notlocked_mapping_table_names,
                         describe_default_entity, entity_name="mapping table")
        migrate_entities(source_db, target_db, ATTACHMENT_COLLECTION, notlocked_attachment_names, describe_attachment_entity,
                         entity_name="attachment", name_field="refName")
    else:
        print("*** Dryrun selected, no actual migration will take place.")


def run(parsed_args: argparse.Namespace):
    source = parsed_args.source
    target = parsed_args.target
    target_db_name = parsed_args.targetdb
    source_db_name = parsed_args.sourcedb

    dryrun = args.dryrun  # if set, only migration description will be printed, no actual migration will run

    print('Menas mongo migration')
    print('Running with settings: dryrun={}, verbose={}'.format(dryrun, verbose))
    print("Using migration #: '{}' and locking timestamp {} (UTC)".format(migration_hash, utc_now))  # script-global vars
    print('  source connection-string: {}'.format(source))
    print('  source DB: {}'.format(source_db_name))
    print('  target connection-string: {}'.format(target))
    print('  target DB: {}'.format(target_db_name))

    source_db = get_database(source, source_db_name)
    target_db = get_database(target, target_db_name)

    # todo do target checking, too when we have menas to create blank db (collections, indices, ...) - Issue #2013
    ensure_db_version(source_db, alias="source db")  # db initialized on source
    ensure_db_version(target_db, alias="target db")  # db initialized on source
    ensure_menas_collections_exist(source_db, alias="source db")  # migrating collections existence on source

    dataset_names = parsed_args.datasets
    mt_names = parsed_args.mtables
    if dataset_names:
        print('dataset names supplied: {}'.format(dataset_names))
        migrate_collections_by_ds_names(source_db, target_db, dataset_names, dryrun=dryrun)
    elif mt_names:
        print('mapping table names supplied: {}'.format(mt_names))
        migrate_collections_by_mt_names(source_db, target_db, mt_names, dryrun=dryrun)
    else:
        # should not happen (-d/-m is exclusive and required)
        raise Exception("Invalid run options: DS names (-d ds1 ds2 ...).. or MT names (-m mt1 mt2 ... ) must be given.")

    print("Done.")


if __name__ == '__main__':
    args = parse_args()

    # globals script vars
    migration_hash = secrets.token_hex(3)  # e.g. 34d4e10f
    utc_now = datetime.now(timezone.utc)  # in order to have same timestamp for the whole script run
    verbose = args.verbose

    run(args)

    # example test-runs:
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -v -d mydataset1 -t menas_target
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -d mydataset1 test654 -t menas2
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -m MyAwesomeMappingTable1 -t msn2
