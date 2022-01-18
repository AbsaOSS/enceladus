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

from pymongo.errors import DuplicateKeyError
from typing import List
from datetime import datetime, timezone

from constants import *
from menas_db import MenasDb

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


def _get_date_locked_structure(dt: datetime) -> dict:
    return {
        "dateTime": {
            "date": {"year": dt.year, "month": dt.month, "day": dt.day},
            "time": {"hour": dt.hour, "minute": dt.minute, "second": dt.second, "nano": dt.microsecond * 1000}
        },
        "offset": 0,
        "zone": "UTC"
    }


def migrate_entities(source_db: MenasDb, target_db: MenasDb, collection_name: str, entity_names_list: List[str],
                     describe_fn, entity_name: str = "entity", name_field: str = "name") -> None:
    if not entity_names_list:
        print("No {}s to migrate in {}, skipping.".format(entity_name, collection_name))
        return

    print("Migration of collection {} started".format(collection_name))
    dataset_collection = source_db.mongodb[collection_name]

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
            "dateLocked": _get_date_locked_structure(utc_now)  # script-global var
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
    target_dataset_collection = target_db.mongodb[collection_name]
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
                {"migrationHash": migration_hash},  # script-global var
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


def migrate_collections_by_ds_names(source_db: MenasDb, target_db: MenasDb,
                                    supplied_ds_names: List[str],
                                    dryrun: bool) -> None:

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names = source_db.get_distinct_ds_names_from_ds_names(supplied_ds_names, not_locked_only=False)
    notlocked_ds_names = source_db.get_distinct_ds_names_from_ds_names(supplied_ds_names, not_locked_only=True)
    print('Dataset names to migrate: {}'.format(notlocked_ds_names))

    ds_schema_names = source_db.assemble_schemas_from_ds_names(ds_names, not_locked_only=False)
    notlocked_ds_schema_names = source_db.assemble_schemas_from_ds_names(ds_names, not_locked_only=True)
    print('DS schemas to migrate: {}'.format(notlocked_ds_schema_names))

    notlocked_mapping_table_names = source_db.assemble_notlocked_mapping_tables_from_ds_names(ds_names)
    mapping_table_names = source_db.get_distinct_mapping_tables_from_ds_names(ds_names, not_locked_only=False)
    print('MTs to migrate: {}'.format(notlocked_mapping_table_names))

    mt_schema_names = source_db.assemble_schemas_from_mt_names(mapping_table_names, not_locked_only=False)
    # final MT schemas must be retrieved from locked MTs, too, not just notlocked_mapping_table_names
    notlocked_mt_schema_names = source_db.assemble_schemas_from_mt_names(mapping_table_names, not_locked_only=True)
    print('MT schemas to migrate: {}'.format(notlocked_mt_schema_names))

    run_unique_ids = source_db.assemble_notlocked_runs_from_ds_names(ds_names)
    print('Runs to migrate: {}'.format(run_unique_ids))

    all_notlocked_schemas = list(set.union(set(notlocked_ds_schema_names), set(notlocked_mt_schema_names)))
    if verbose:
        print('All schemas (DS & MT) to migrate: {}'.format(all_notlocked_schemas))

    # attachments from locked schemas, too:
    schemas_names_for_attachments = list(set.union(set(ds_schema_names), set(mt_schema_names)))  # locked+unlocked
    notlocked_attachment_names = source_db.assemble_notlocked_attachments_from_schema_names(schemas_names_for_attachments)
    print('Attachments of schemas to migrate: {}'.format(notlocked_attachment_names))

    if not dryrun:
        print("")
        migrate_entities(source_db, target_db, SCHEMA_COLLECTION, all_notlocked_schemas, describe_default_entity, entity_name="schema")
        migrate_entities(source_db, target_db, DATASET_COLLECTION, notlocked_ds_names, describe_default_entity, entity_name="dataset")
        migrate_entities(source_db, target_db, MAPPING_TABLE_COLLECTION, notlocked_mapping_table_names,
                         describe_default_entity, entity_name="mapping table")
        migrate_entities(source_db, target_db, RUN_COLLECTION, run_unique_ids, describe_run_entity, entity_name="run", name_field="uniqueId")
        migrate_entities(source_db, target_db, ATTACHMENT_COLLECTION, notlocked_attachment_names,
                         describe_attachment_entity, entity_name="attachment", name_field="refName")
    else:
        print("*** Dryrun selected, no actual migration will take place.")


def migrate_collections_by_mt_names(source_db: MenasDb, target_db: MenasDb,
                                    supplied_mt_names: List[str],
                                    dryrun: bool) -> None:
    if verbose:
        print("MT names given: {}".format(supplied_mt_names))

    notlocked_mapping_table_names = source_db.assemble_mapping_tables_from_mt_names(supplied_mt_names, not_locked_only=True)
    print('MTs to migrate: {}'.format(notlocked_mapping_table_names))

    notlocked_mt_schema_names = source_db.assemble_schemas_from_mt_names(supplied_mt_names, not_locked_only=True)
    print('MT schemas to migrate: {}'.format(notlocked_mt_schema_names))

    mt_schema_names = source_db.assemble_schemas_from_mt_names(supplied_mt_names, not_locked_only=False)
    notlocked_attachment_names = source_db.assemble_notlocked_attachments_from_schema_names(mt_schema_names)
    print('Attachments of schemas to migrate: {}'.format(notlocked_attachment_names))

    if not dryrun:
        print("")
        migrate_entities(source_db, target_db, SCHEMA_COLLECTION, notlocked_mt_schema_names, describe_default_entity, entity_name="schema")
        migrate_entities(source_db, target_db, MAPPING_TABLE_COLLECTION, notlocked_mapping_table_names,
                         describe_default_entity, entity_name="mapping table")
        migrate_entities(source_db, target_db, ATTACHMENT_COLLECTION, notlocked_attachment_names, describe_attachment_entity,
                         entity_name="attachment", name_field="refName")
    else:
        print("*** Dryrun selected, no actual migration will take place.")


def run(parsed_args: argparse.Namespace):
    source_conn_string = parsed_args.source
    target_conn_string = parsed_args.target
    target_db_name = parsed_args.targetdb
    source_db_name = parsed_args.sourcedb

    dryrun = args.dryrun  # if set, only migration description will be printed, no actual migration will run

    print('Menas mongo migration')
    print('Running with settings: dryrun={}, verbose={}'.format(dryrun, verbose))
    print("Using migration #: '{}' and locking timestamp {} (UTC)".format(migration_hash, utc_now))  # script-global vars
    print('  source connection-string: {}'.format(source_conn_string))
    print('  source DB: {}'.format(source_db_name))
    print('  target connection-string: {}'.format(target_conn_string))
    print('  target DB: {}'.format(target_db_name))

    source_db = MenasDb.from_connection_string(source_conn_string, source_db_name, alias="source db", verbose=verbose)
    target_db = MenasDb.from_connection_string(target_conn_string, target_db_name, alias="target db", verbose=verbose)

    # Checks raise MenasDbErrors
    print("Checking source db validity...")
    source_db.check_db_version()
    source_db.check_menas_collections_exist()

    print("Checking target db validity...")
    target_db.check_db_version()
    target_db.check_menas_collections_exist()

    dataset_names = parsed_args.datasets
    mt_names = parsed_args.mtables
    if dataset_names:
        print('Dataset names supplied: {}'.format(dataset_names))
        migrate_collections_by_ds_names(source_db, target_db, dataset_names, dryrun=dryrun)
    elif mt_names:
        print('Mapping table names supplied: {}'.format(mt_names))
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
