#!/usr/bin/env python3

# Copyright 2023 ABSA Group Limited
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

from typing import List

from constants import *
from menas_db import MenasDb
from dataclasses import dataclass

# python package needed are denoted in requirements.txt, so to fix missing dependencies, just run
# pip install -r requirements.txt


DEFAULT_SKIP_PREFIXES = []
DEFAULT_DATASETS_ONLY = False

MAPPING_FIELD_HDFS_PATH = "hdfsPath"
MAPPING_FIELD_HDFS_PUBLISH_PATH = "hdfsPublishPath"
MAPPING_FIELD_HDFS_ALL = "all"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='dataset_paths_ecs_rollback',
        description='Menas MongoDB rollback script changes to ECS',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # prints default values, too, on help (-h)
    )

    parser.add_argument('-n', '--dryrun', action='store_true', default=DEFAULT_DRYRUN,
                        help="If specified, skip the actual changes, just print what would be done.")
    parser.add_argument('-v', '--verbose', action="store_true", default=DEFAULT_VERBOSE,
                        help="Prints extra information while running.")

    parser.add_argument('target', metavar="TARGET", help="connection string for target MongoDB")
    parser.add_argument('-t', '--target-database', dest="targetdb", default=DEFAULT_DB_NAME,
                        help="Name of db on target to be affected.")

    parser.add_argument('-s', '--skip-prefixes', dest="skipprefixes", metavar="SKIP_PREFIX", default=DEFAULT_SKIP_PREFIXES,
                        nargs="+", help="Paths with these prefixes will be skipped from rollback.")

    parser.add_argument('-f', '--fields-to-map', dest='fieldstomap', choices=[MAPPING_FIELD_HDFS_PATH, MAPPING_FIELD_HDFS_PUBLISH_PATH, MAPPING_FIELD_HDFS_ALL],
                        default=MAPPING_FIELD_HDFS_ALL, help="Rollback either item's 'hdfsPath', 'hdfsPublishPath' or 'all'.")

    parser.add_argument('-o', '--only-datasets', dest='onlydatasets', action='store_true', default=DEFAULT_DATASETS_ONLY,
                        help="if specified, only dataset rollback path changes will be done (not MTs).")

    input_options_group = parser.add_mutually_exclusive_group(required=True)
    input_options_group.add_argument('-d', '--datasets', dest='datasets', metavar="DATASET_NAME", default=[],
                                     nargs="+", help='list datasets names to rollback path changes in')
    input_options_group.add_argument('-m', '--mapping-tables', dest="mtables", metavar="MTABLE_NAME", default=[],
                                     nargs="+", help='list mapping tables names to rollback path changes in')


    return parser.parse_args()


@dataclass
class RollbackSettings:
    skip_prefixes: List[str]
    fields_to_map: str  # HDFS_MAPPING_FIELD_HDFS_*


def update_data_for_item(item: dict, rollback_settings: RollbackSettings) -> (dict, dict):
    """
    Prepares dicts of set and unset fields based on the item and rollback settings.
    For hdfsPath to be rollbacked, the following conditions must be met:
     - bakHdfsPath must exits on item
     - bakHdfsPath must not start with any of prefixes given in rollback_settings.skip_prefixes
     - rollback_settings.skip_prefixes must be MAPPING_FIELD_HDFS_PATH or MAPPING_FIELD_HDFS_ALL

     For hdfsPublishPath to be rollbacked, the following conditions must be met:
     - bakHdfsPublishPath must exits on item
     - bakHdfsPublishPath must not start with any of prefixes given in rollback_settings.skip_prefixes
     - rollback_settings.skip_prefixes must be MAPPING_FIELD_HDFS_PUBLISH_PATH or MAPPING_FIELD_HDFS_ALL

    :param item:
    :param rollback_settings:
    :return: dict of fields to be set on item, dict of fields to be unset on item
    """
    def starts_with_one_of_prefixes(path: str, prefixes: List[str]) -> bool:
        return any(path.startswith(prefix) for prefix in prefixes)

    data_update_set = {}
    data_update_unset = {}

    # rollback bakHdfsPath -> hdfsPath
    has_bak_hdfs_path = "bakHdfsPath" in item
    if has_bak_hdfs_path:
        bak_hdfs_path = item["bakHdfsPath"]
        hdfs_to_be_rollbacked = True if rollback_settings.fields_to_map in {MAPPING_FIELD_HDFS_PATH, MAPPING_FIELD_HDFS_ALL} else False

        if hdfs_to_be_rollbacked and not starts_with_one_of_prefixes(bak_hdfs_path, rollback_settings.skip_prefixes):
            data_update_set["hdfsPath"] = bak_hdfs_path
            data_update_unset["bakHdfsPath"] = ""

    # rollback bakHdfsPublishPath -> hdfsPublishPath
    has_bak_hdfs_publish_path = "bakHdfsPublishPath" in item
    if has_bak_hdfs_publish_path:
        bak_hdfs_publish_path = item["bakHdfsPublishPath"]
        hdfs_publish_to_be_rollbacked = True if rollback_settings.fields_to_map in {MAPPING_FIELD_HDFS_PUBLISH_PATH, MAPPING_FIELD_HDFS_ALL} else False

        if hdfs_publish_to_be_rollbacked and not starts_with_one_of_prefixes(bak_hdfs_publish_path, rollback_settings.skip_prefixes):
            data_update_set["hdfsPublishPath"] = bak_hdfs_publish_path
            data_update_unset["bakHdfsPublishPath"] = ""

    return data_update_set, data_update_unset


def data_update_to_nice_string(data_update: dict) -> str:
    mappings = []

    # hdfsPath
    if "hdfsPath" in data_update:
        hdfs_path = data_update["hdfsPath"]
        mappings.append(f"hdfsPath: -> {hdfs_path}")

    # hdfsPublishPath
    if "hdfsPublishPath" in data_update:
        hdfs_publish_path = data_update["hdfsPublishPath"]
        mappings.append(f"hdfsPublishPath: -> {hdfs_publish_path}")

    return ", ".join(mappings)

def rollback_pathchange_entities(target_db: MenasDb, collection_name: str, entity_name: str, entity_names_list: List[str],
                                 mapping_settings: RollbackSettings, dryrun: bool) -> None:

    assert entity_name == "dataset" or entity_name == "mapping table" , "this method supports datasets and MTs only!"

    if not entity_names_list:
        print("No {}s to rollback path-changes in {}, skipping.".format(entity_name, collection_name))
        return

    print("Rollbacking path-change of collection {} started".format(collection_name))
    dataset_collection = target_db.mongodb[collection_name]

    query =  {"$and": [
        {"name": {"$in": entity_names_list}}  # dataset/MT name
    ]}

    docs_count = dataset_collection.count_documents(query)
    docs = dataset_collection.find(query)

    print("Found: {} {} documents for a potential path change. In progress ... ".format(docs_count, entity_name))

    patched = 0
    failed_count = 0
    for item in docs:
        # item preview
        if verbose:
            print("Rollbacking path changes for {} '{}' v{} (_id={}).".format(entity_name, item["name"], item["version"], item["_id"]))

        update_data_set, update_data_unset = update_data_for_item(item, mapping_settings)
        if update_data_set != {} and update_data_unset != {}:
            if dryrun:
                print("  *would rollback* {}".format(data_update_to_nice_string(update_data_set)))
                print("")

            else:
                try:
                    if verbose:
                        print("  *rollbacking*: {}".format(data_update_to_nice_string(update_data_set)))

                    update_result = dataset_collection.update_one(
                        {"$and": [
                            {"_id": item["_id"]}
                        ]},
                        {"$set": update_data_set, "$unset": update_data_unset}
                    )
                    if update_result.acknowledged and verbose:
                        print("Successfully rollbacked changed path for {} '{}' v{} (_id={}).".format(entity_name, item["name"], item["version"], item["_id"]))
                        print("")

                except Exception as e:
                    print("Warning: Error while change-path rollbacking for {} '{}' v{} (_id={}): {}".format(entity_name, item["name"], item["version"], item["_id"], e))
                    failed_count += 1
                else:
                    patched += 1
        else:
            if verbose:
                print("Nothing left to rollback for {} '{}' v{} (_id={}).".format(entity_name, item["name"], item["version"], item["_id"]))

    print("Successfully rollbacked {} of {} {} entries, failed: {}".format(patched, docs_count, entity_name, failed_count))
    print("")


def rollback_pathchange_collections_by_ds_names(target_db: MenasDb,
                                                supplied_ds_names: List[str],
                                                mapping_settings: RollbackSettings,
                                                onlydatasets: bool,
                                                dryrun: bool) -> None:

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names_found = target_db.get_distinct_ds_names_from_ds_names(supplied_ds_names, migration_free_only=False)
    print('Dataset names to rollback path-changes (actually found db): {}'.format(ds_names_found))

    if onlydatasets:
        print("Configured *NOT* to rollback path-changes for related mapping tables.")
    else:
        mapping_table_found_for_dss = target_db.get_distinct_mapping_tables_from_ds_names(ds_names_found, migration_free_only=False)
        print('MTs for path-change rollback: {}'.format(mapping_table_found_for_dss))


    print("")
    rollback_pathchange_entities(target_db, DATASET_COLLECTION, "dataset", ds_names_found, mapping_settings, dryrun)
    if not onlydatasets:
        rollback_pathchange_entities(target_db, MAPPING_TABLE_COLLECTION, "mapping table", mapping_table_found_for_dss, mapping_settings, dryrun)



def rollback_pathchange_collections_by_mt_names(target_db: MenasDb,
                                                supplied_mt_names: List[str],
                                                mapping_settings: RollbackSettings,
                                                dryrun: bool) -> None:

    if verbose:
        print("Mapping table names given: {}".format(supplied_mt_names))

    mt_names_found = target_db.get_distinct_mt_names_from_mt_names(supplied_mt_names, migration_free_only=False)
    print('Mapping table names to rollback path-changes (actually found db): {}'.format(mt_names_found))

    print("")
    rollback_pathchange_entities(target_db, MAPPING_TABLE_COLLECTION, "mapping table", mt_names_found, mapping_settings, dryrun)


def run(parsed_args: argparse.Namespace):
    target_conn_string = parsed_args.target
    target_db_name = parsed_args.targetdb

    dryrun = args.dryrun  # if set, only path change rollback description will be printed, no actual patching will run

    skip_prefixes = args.skipprefixes
    fields_to_map = args.fieldstomap # argparse allow only one of HDFS_MAPPING_FIELD_HDFS_*
    rollback_settings  = RollbackSettings(skip_prefixes, fields_to_map)

    print('Menas mongo ECS paths mapping ROLLBACK')
    print('Running with settings: dryrun={}, verbose={}'.format(dryrun, verbose))
    print('Skipping prefixes: {}'.format(skip_prefixes))
    print('  target connection-string: {}'.format(target_conn_string))
    print('  target DB: {}'.format(target_db_name))
    target_db = MenasDb.from_connection_string(target_conn_string, target_db_name, alias="target db", verbose=verbose)

    dataset_names = parsed_args.datasets
    only_datasets = parsed_args.onlydatasets
    mt_names = parsed_args.mtables

    if dataset_names:
        print('Dataset names supplied: {}'.format(dataset_names))
        rollback_pathchange_collections_by_ds_names(target_db, dataset_names, rollback_settings, only_datasets, dryrun=dryrun)

    elif mt_names:
        if only_datasets:
            raise Exception("Invalid run options: -o/--only-datasets cannot be used with -m/--mapping-tables, only for -d/--datasets")

        print('Mapping table names supplied: {}'.format(mt_names))
        rollback_pathchange_collections_by_mt_names(target_db, mt_names, rollback_settings, dryrun=dryrun)

    else:
        # should not happen (-d/-m is exclusive and required)
        raise Exception("Invalid run options: DS names (-d ds1 ds2 ...)..,  MT names (-m mt1 mt2 ... ) must be given.")

    print("Done.")
    print("")


if __name__ == '__main__':
    args = parse_args()

    # globals script vars
    verbose = args.verbose
    run(args)

    ## Examples runs:
    # Dry-run example:
    # python dataset_paths_ecs_rollback.py mongodb://localhost:27017/admin -d MyDataset1 AnotherDatasetB -t menas_remap_test -n
    # Verbose run example
    # python dataset_paths_ecs_rollback.py mongodb://localhost:27017/admin -d DatasetA -t menas_remap_test -v
