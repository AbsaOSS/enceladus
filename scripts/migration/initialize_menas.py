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

import pymongo
from pymongo.database import Database
from typing import List

import constants
from menas_db import MenasDb
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

    # create db_version record
    print("'db_version' initialization:")
    menas_db.create_db_version(silent=False)

    # create necessary collections
    create_collections_if_not_exist(menas_db.mongodb, MIGRATING_COLLECTIONS)

    # create necessary indices
    for collection in MIGRATING_COLLECTIONS:
        indices = INDICES.get(collection, None)  # returns None if key is not found
        if indices:
            print(f"Initialization indices ({len(indices)}) on Menas collection '{collection}':")
            initialize_collection_indices(menas_db.mongodb, collection, indices)
        else:
            print(f"Collection {collection} needs no index setup.")
        print("")


def create_collections_if_not_exist(db: Database, collection_names: List[str]):
    print("Initialization of migration-related collections")
    existing_collections = db.list_collection_names()
    if verbose:
        print(f"  BEFORE: Aiming to create {collection_names}, found exising {existing_collections}.")

    left_to_create = list(filter(lambda col_name: not(col_name in existing_collections), collection_names))
    print(f"  Creating missing collections {left_to_create} ...")
    for missing_col in left_to_create:
        db.create_collection(missing_col)

    if verbose:
        print(f"  AFTER: existing collections: {db.list_collection_names()}")
    print("")


def initialize_collection_indices(db: Database, name: str, indices: list):
    # db.create_collection(name) # may raise if exits, should not be needed? # todo test this
    col = db[name]
    if verbose:
        print(f"  BEFORE: index info: {col.index_information()}")

    # Example index info:
    # index info: {'_id_': {'v': 2, 'key': [('_id', 1)], 'ns': 'menas.run_v1'}, 'dataset_1_datasetVersion_1_runId_1':
    # {'v': 2, 'unique': True, 'key': [('dataset', 1), ('datasetVersion', 1), ('runId', 1)], 'ns': 'menas.run_v1'}}

    # existing indices = no-op
    for desired_index in indices:
        key = desired_index[INDICES_FIELD_KEY]
        unique = desired_index[INDICES_FIELD_UNIQUE]
        print(f"  creating index for key {key}, unique={unique}")
        col.create_index(key, unique=unique)

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

    from menas_db import MenasDbError, MenasDb
    target_db: MenasDb = MenasDb.from_connection_string(target, target_db_name, "target db", verbose)

    try:
        target_db.check_db_version()
        target_db.check_menas_collections_exist()  # todo, check indexing, too?

    except MenasDbError as err:
        print(f"DB '{target_db_name}' does not seem as valid menas DB.")
        print(f"  {type(err).__name__}: {err}\n")
        if dryrun:
            print("*** Dry run: no actual initialization performed.\n")  # todo sufficient?
        else:
            print(f"Initializing DB '{target_db_name}' for Menas...\n")
            initialize_menas_db(target_db)
    else:
        print(f"Db '{target_db_name}' init check successful, no initialization needed.")

    print("\nDone.")


if __name__ == '__main__':
    args = parse_args()

    # globals script vars
    verbose = args.verbose

    run(args)

    # example test-runs:
    # TODO
