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
import requests

# python package needed are denoted in requirements.txt, so to fix missing dependencies, just run
# pip install -r requirements.txt


DEFAULT_MAPPING_SERVICE_URL = "https://set-your-mapping-service-here.execute-api.af-south-1.amazonaws.com/dev/map"

DEFAULT_MAPPING_PREFIX = "s3a://"
DEFAULT_SKIP_PREFIXES = ["s3a://", "/tmp"]
DEFAULT_DATASETS_ONLY = False

MAPPING_FIELD_HDFS_PATH = "hdfsPath"
MAPPING_FIELD_HDFS_PUBLISH_PATH = "hdfsPublishPath"
MAPPING_FIELD_HDFS_ALL = "all"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='dataset_paths_to_ecs',
        description='Menas MongoDB path changes to ECS',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # prints default values, too, on help (-h)
    )

    parser.add_argument('-n', '--dryrun', action='store_true', default=DEFAULT_DRYRUN,
                        help="if specified, skip the actual changes, just print what would be done.")
    parser.add_argument('-v', '--verbose', action="store_true", default=DEFAULT_VERBOSE,
                        help="prints extra information while running.")

    parser.add_argument('target', metavar="TARGET", help="connection string for target MongoDB")
    parser.add_argument('-t', '--target-database', dest="targetdb", default=DEFAULT_DB_NAME,
                        help="Name of db on target to be affected.")

    parser.add_argument('-u', '--mapping-service-url', dest="mappingservice", default=DEFAULT_MAPPING_SERVICE_URL,
                        help="Service URL to use for path change mapping.")

    parser.add_argument('-p', '--mapping-prefix', dest="mappingprefix", default=DEFAULT_MAPPING_PREFIX,
                        help="This prefix will be prepended to mapped path by the Mapping service")

    parser.add_argument('-s', '--skip-prefixes', dest="skipprefixes", metavar="SKIP_PREFIX", default=DEFAULT_SKIP_PREFIXES,
                        nargs="+", help="Path with these prefixes will be skipped from mapping")

    parser.add_argument('-f', '--fields-to-map', dest='fieldstomap', choices=[MAPPING_FIELD_HDFS_PATH, MAPPING_FIELD_HDFS_PUBLISH_PATH, MAPPING_FIELD_HDFS_ALL],
                        default=MAPPING_FIELD_HDFS_ALL, help="Map either item's 'hdfsPath', 'hdfsPublishPath' or 'all'")

    parser.add_argument('-d', '--datasets', dest='datasets', metavar="DATASET_NAME", default=[],
                        nargs="+", help='list datasets names to change paths in')

    parser.add_argument('-o', '--only-datasets', dest='onlydatasets', action='store_true', default=DEFAULT_DATASETS_ONLY,
                        help="if specified, mapping table changes will NOT be done.")


    return parser.parse_args()

def map_path_from_svc(path: str, path_prefix_to_add: str, svc_url: str)-> str:
    # Example usage of the service:
    # curl -X GET -d '{"hdfs_path":"/bigdatahdfs/datalake/publish/dm9/CNSMR_ACCNT/country_code=KEN"}' 'https://my_service.amazonaws.com/dev/map'
    # {"ecs_path": "ursamajor123-abs1234-prod-edla-abc123-ke/publish/CNSMR_ACCNT/country_code=KEN/"}

    payload = "{\"hdfs_path\":\"" + path + "\"}"
    response = requests.get(svc_url, data=payload)

    if response.status_code != 200:
        raise Exception(f"Could load ECS path from {svc_url} for hdfs_path='{path}', received error {response.status_code} {response.text}")

    wrapper = response.json()
    ecs_path = wrapper['ecs_path']

    return path_prefix_to_add + ecs_path


@dataclass
class MappingSettings:
    mapping_service_url: str
    mapping_prefix: str
    skip_prefixes: List[str]
    fields_to_map: str  # HDFS_MAPPING_FIELD_HDFS_*


def update_data_for_item(item: dict, mapping_settings: MappingSettings) -> dict:
    # hdfsPath
    hdfs_to_be_path_changed = True if mapping_settings.fields_to_map in {MAPPING_FIELD_HDFS_PATH, MAPPING_FIELD_HDFS_ALL} else False
    hdfs_path = item["hdfsPath"]

    def starts_with_one_of_prefixes(path: str, prefixes: List[str]) -> bool:
        return any(path.startswith(prefix) for prefix in prefixes)

    if hdfs_to_be_path_changed and not starts_with_one_of_prefixes(hdfs_path, mapping_settings.skip_prefixes):
        updated_hdfs_path = map_path_from_svc(hdfs_path, mapping_settings.mapping_prefix, mapping_settings.mapping_service_url)
        data_update = {
            "hdfsPath": updated_hdfs_path,
            "bakHdfsPath": hdfs_path
        }
    else:
        # not mapped
        data_update = {}

    # hdfsPublishPath
    has_hdfs_publish_path = "hdfsPublishPath" in item
    if has_hdfs_publish_path:
        hdfs_publish_path = item["hdfsPublishPath"]
        hdfs_publish_to_be_path_changed = True if mapping_settings.fields_to_map in {MAPPING_FIELD_HDFS_PUBLISH_PATH, MAPPING_FIELD_HDFS_ALL} else False

        if hdfs_publish_to_be_path_changed and not starts_with_one_of_prefixes(hdfs_publish_path, mapping_settings.skip_prefixes):
            updated_hdfs_publish_path = map_path_from_svc(hdfs_publish_path, mapping_settings.mapping_prefix, mapping_settings.mapping_service_url)
            data_update["hdfsPublishPath"] = updated_hdfs_publish_path
            data_update["bakHdfsPublishPath"] = hdfs_publish_path

    return data_update

def data_update_to_nice_string(data_update: dict) -> str:
    mappings = []

    # hdfsPath
    if "hdfsPath" in data_update and "bakHdfsPath" in data_update:
        hdfs_path = data_update["hdfsPath"]
        bak_hdfs_path = data_update["bakHdfsPath"]
        mappings.append(f"hdfsPath: {bak_hdfs_path} -> {hdfs_path}")

    # hdfsPublishPath
    if "hdfsPublishPath" in data_update and "bakHdfsPublishPath" in data_update:
        hdfs_publish_path = data_update["hdfsPublishPath"]
        bak_hdfs_publish_path = data_update["bakHdfsPublishPath"]
        mappings.append(f"hdfsPublishPath: {bak_hdfs_publish_path} -> {hdfs_publish_path}")

    return ", ".join(mappings)

def pathchange_entities(target_db: MenasDb, collection_name: str, entity_name: str, entity_names_list: List[str],
                        mapping_settings: MappingSettings, dryrun: bool) -> None:

    assert entity_name == "dataset" or entity_name == "mapping table" , "this method supports datasets and MTs only!"

    if not entity_names_list:
        print("No {}s to path-change in {}, skipping.".format(entity_name, collection_name))
        return

    print("Path changing of collection {} started".format(collection_name))
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
            print("Changing paths for {} '{}' v{} (_id={}).".format(entity_name, item["name"], item["version"], item["_id"]))

        update_data = update_data_for_item(item, mapping_settings)
        if update_data != {}:
            if dryrun:
                print("  *would set* {}".format(data_update_to_nice_string(update_data)))
                print("")

            else:
                try:
                    if verbose:
                        print("  *changing*: {}".format(data_update_to_nice_string(update_data)))

                    update_result = dataset_collection.update_one(
                        {"$and": [
                            {"_id": item["_id"]}
                        ]},
                        {"$set": update_data}
                    )
                    if update_result.acknowledged and verbose:
                        print("Successfully changed path for {} '{}' v{} (_id={}).".format(entity_name, item["name"], item["version"], item["_id"]))
                        print("")

                except Exception as e:
                    print("Warning: Error while changing paths for {} '{}' v{} (_id={}): {}".format(entity_name, item["name"], item["version"], item["_id"], e))
                    failed_count += 1
                else:
                    patched += 1
        else:
            if verbose:
                print("Nothing left to change for {} '{}' v{} (_id={}).".format(entity_name, item["name"], item["version"], item["_id"]))

    print("Successfully migrated {} of {} {} entries, failed: {}".format(patched, docs_count, entity_name, failed_count))
    print("")


def pathchange_collections_by_ds_names(target_db: MenasDb,
                                       supplied_ds_names: List[str],
                                       mapping_settings: MappingSettings,
                                       onlydatasets: bool,
                                       dryrun: bool) -> None:

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names_found = target_db.get_distinct_ds_names_from_ds_names(supplied_ds_names, migration_free_only=False)
    print('Dataset names to path change (actually found db): {}'.format(ds_names_found))

    if onlydatasets:
        print("Configured *NOT* to path-change related mapping tables.")
    else:
        mapping_table_found_for_dss = target_db.get_distinct_mapping_tables_from_ds_names(ds_names_found, migration_free_only=False)
        print('MTs to path change: {}'.format(mapping_table_found_for_dss))


    print("")
    pathchange_entities(target_db, DATASET_COLLECTION, "dataset", ds_names_found, mapping_settings, dryrun)
    if not onlydatasets:
        pathchange_entities(target_db, MAPPING_TABLE_COLLECTION, "mapping table", mapping_table_found_for_dss, mapping_settings, dryrun)

def pre_run_mapping_service_check(svc_url: str, path_prefix_to_add: str):
    test_path = "/bigdatahdfs/datalake/publish/pcub/just/a/path/to/test/the/service/"

    map_path_from_svc(test_path, path_prefix_to_add, svc_url)

def run(parsed_args: argparse.Namespace):
    target_conn_string = parsed_args.target
    target_db_name = parsed_args.targetdb

    dryrun = args.dryrun  # if set, only path change description will be printed, no actual patching will run

    mapping_service = args.mappingservice
    mapping_prefix = args.mappingprefix
    skip_prefixes = args.skipprefixes
    fields_to_map = args.fieldstomap # argparse allow only one of HDFS_MAPPING_FIELD_HDFS_*
    mapping_settings  = MappingSettings(mapping_service, mapping_prefix, skip_prefixes, fields_to_map)

    print('Menas mongo ECS paths mapping')
    print('Running with settings: dryrun={}, verbose={}'.format(dryrun, verbose))
    print("Using mapping service at: {}".format(mapping_service))
    print('  target connection-string: {}'.format(target_conn_string))
    print('  target DB: {}'.format(target_db_name))

    print('Testing mapping service availability first...')
    pre_run_mapping_service_check(mapping_service, mapping_prefix)  # would throw on error, let's fail fast before mongo is tried
    print('  ok')
    print("")

    target_db = MenasDb.from_connection_string(target_conn_string, target_db_name, alias="target db", verbose=verbose)

    dataset_names = parsed_args.datasets
    only_datasets = parsed_args.onlydatasets
    pathchange_collections_by_ds_names(target_db, dataset_names, mapping_settings, only_datasets, dryrun=dryrun)

    print("Done.")
    print("")


if __name__ == '__main__':
    args = parse_args()

    # globals script vars
    verbose = args.verbose
    run(args)

    ## Examples runs:
    # Dry-run example:
    # python dataset_paths_to_ecs.py mongodb://localhost:27017/admin -d MyDataset1 AnotherDatasetB -t menas_remap_test -n -u https://my_service.amazonaws.com/dev/map
    # Verbose run example, will use DEFAULT_MAPPING_SERVICE_URL on line 28:
    # python dataset_paths_to_ecs.py mongodb://localhost:27017/admin -d DatasetA -t menas_remap_test -v
