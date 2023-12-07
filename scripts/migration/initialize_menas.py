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

import argparse

from pymongo.database import Database
from typing import List

from menas_db import MenasDb, MenasDbVersionError, MenasNoDbVersionRecordError, MenasDbCollectionError
from constants import *


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='initialize_menas',
        description='Menas MongoDB initialization script.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # prints default values, too, on help (-h)
    )

    parser.add_argument('-n', '--dryrun', action='store_true', default=DEFAULT_DRYRUN,
                        help="if specified, skip the actual synchronization, just print what would be copied over.")
    parser.add_argument('-v', '--verbose', action="store_true", default=DEFAULT_VERBOSE,
                        help="prints extra information while running.")

    parser.add_argument('target', metavar="TARGET", help="connection string for target MongoDB")

    parser.add_argument('-t', '--target-database', dest="targetdb", default=DEFAULT_DB_NAME,
                        help="Name of db on target to migrate to.")

    return parser.parse_args()


def initialize_menas_db(menas_db: MenasDb) -> None:
    # create necessary collections
    create_collections(menas_db.mongodb, ALL_COLLECTIONS)

    # create necessary indices
    for collection in ALL_COLLECTIONS:
        indices = INDICES.get(collection, None)  # returns None if key is not found
        if indices:
            print(f"Initialization indices ({len(indices)}) on Menas collection '{collection}':")
            initialize_collection_indices(menas_db.mongodb, collection, indices)
        else:
            print(f"Collection {collection} needs no index setup.")
        print("")

    # create db_version record - last as a sign of correctly initialized Menas DB
    print("'db_version' initialization:")
    create_db_version(menas_db.mongodb)


def create_db_version(db: Database) -> None:
    """
    Adds db-record having version=1 into collection #DB_VERSION_COLLECTION
    throws assertion error if there already exits a version record
    """

    collection = db[DB_VERSION_COLLECTION]
    version_record = collection.find_one()
    assert version_record is None, \
        "Precondition failed: MenasDb.create_db_version may only be run if the there is no db-record"

    insert_result = collection.insert_one({"version": 1})
    print(f"  Created version=1 record with id {insert_result.inserted_id}")


def create_collections(db: Database, collection_names: List[str]):
    print("Initialization of migration-related collections")
    existing_collections = db.list_collection_names()
    existing_menas_collections = db.list_collection_names(filter={
        "name": {"$in": ALL_COLLECTIONS}
    })

    if len(existing_menas_collections) != 0:
        raise MenasDbCollectionError("Failed to safely initialize Menas collections. The init script found existing "
                                     f"menas collections in the uninitialized DB: {existing_menas_collections}")
    if len(existing_collections) != 0:
        print(f"  Warning, there are (non-conflicting) collections in the DB already: {existing_collections}")

    if verbose:
        print(f"  BEFORE: Aiming to create {collection_names}, found already exising {existing_collections}.")

    print(f"  Creating collections {collection_names} ...")
    for col_name in collection_names:
        db.create_collection(col_name)

    if verbose:
        print(f"  AFTER: existing collections: {db.list_collection_names()}")
    print("")


def initialize_collection_indices(db: Database, name: str, indices: List[dict]):
    col = db[name]
    if verbose:
        print(f"  BEFORE: index info: {col.index_information()}")

    # Example index info:
    # index info: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'menas.run_v1'}, 'dataset_1_datasetVersion_1_runId_1':
    # {'v': 2, 'unique': True, 'key': [('dataset', 1), ('datasetVersion', 1), ('runId', 1)], 'ns': 'menas.run_v1'}}

    # existing indices = no-op
    for desired_index in indices:
        key = desired_index[INDICES_FIELD_KEY]
        unique = desired_index.get(INDICES_FIELD_UNIQUE, None)
        sparse = desired_index.get(INDICES_FIELD_SPARSE, None)

        kwargs = {}
        if unique is not None:
            kwargs[INDICES_FIELD_UNIQUE] = unique
        if sparse is not None:
            kwargs[INDICES_FIELD_SPARSE] = sparse

        print(f"  creating index for key {key}, unique={unique}, sparse = {sparse}")
        col.create_index(key, **kwargs)

    if verbose:
        print(f"  AFTER: index info: {col.index_information()}")


def run(parsed_args: argparse.Namespace):
    target = parsed_args.target
    target_db_name = parsed_args.targetdb

    dryrun = args.dryrun  # if set, only migration description will be printed, no actual migration will run

    print('Menas mongo initialization')
    print('Running with settings: dryrun={}, verbose={}'.format(dryrun, verbose))
    print('  target connection-string: {}'.format(target))
    print('  target DB: {}'.format(target_db_name))
    print("")

    target_db: MenasDb = MenasDb.from_connection_string(target, target_db_name, "target db", verbose)

    try:
        target_db.check_db_version()
    except MenasNoDbVersionRecordError as version_record_err:  # empty (-like db)
        print(f"DB '{target_db_name}' does not contain record version ({version_record_err})\n")
        if dryrun:
            print("*** Dry run: no actual initialization performed.\n")
        else:
            print(f"Initializing DB '{target_db_name}' for Menas...\n")
            initialize_menas_db(target_db)

    except MenasDbVersionError as version_err:  # a record exists in collection
        found_version = version_err.found_version
        if found_version is None:
            print(f"DB '{target_db_name}' contains version record, but it is invalid: {version_err}.\n")
            exit(10)
        else:
            assert found_version != 1  # this would not be a valid MenasDbVersionError
            if found_version < 1:
                print(f"DB '{target_db_name}' is too old (version={found_version}), run Menas to update it.")
                exit(11)
            else:
                print(f"DB '{target_db_name}' is too new (version={found_version}), not supported by this script.")
                exit(12)
    else:
        print(f"Db '{target_db_name}' db-version check successful, no initialization needed.")

    print("\nDone.")


if __name__ == '__main__':
    args = parse_args()

    # globals script vars
    verbose = args.verbose

    run(args)

    # example test-runs:
    # .\initialize_menas.py  mongodb://localhost:27017/admin -t mns2
