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

from typing import List

from constants import *
from menas_db import MenasDb, MenasDbCollectionError
import requests

# python package needed are denoted in requirements.txt, so to fix missing dependencies, just run
# pip install -r requirements.txt


DEFAULT_MAPPING_SERVICE_URL = "xxx"
# Example usage of the service:
# curl -X GET -d '{"hdfs_path":"/bigdatahdfs/datalake/publish/dm9/CNSMR_ACCNT/country_code=KEN"}' 'https://my_service.amazonaws.com/dev/map'
# {"ecs_path": "ursamajor123-abs1234-prod-edla-abc123-ke/publish/CNSMR_ACCNT/country_code=KEN/"}

DEFAULT_MAPPING_PREFIX = "s3a://"

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

    parser.add_argument('-s', '--mapping-service', dest="mappingservice", default=DEFAULT_MAPPING_SERVICE_URL,
                        help="Service to use for path change mapping.")

    parser.add_argument('-p', '--mapping-prefix', dest="mappingprefix", default=DEFAULT_MAPPING_PREFIX,
                        help="Default mapping prefix to be applied for paths")

    parser.add_argument('-d', '--datasets', dest='datasets', metavar="DATASET_NAME", default=[],
                        nargs="+", help='list datasets names to change paths in')
    # todo not used now
    parser.add_argument('-m', '--mapping-tables', dest="mtables", metavar="MTABLE_NAME", default=[],
                        nargs="+", help='list mapping tables names to change paths in')

    return parser.parse_args()

def map_path_from_svc(path: str, path_prefix_to_add: str, svc_url: str)-> str:
    # session = requests.Session()
    #response = session.post(url, auth=basic_auth, verify=http_verify)

    payload = "{\"hdfs_path\":\"" + path + "\"}"
    response = requests.get(svc_url, data=payload)

    if response.status_code != 200:
        raise Exception(f"Could load ECS path from {svc_url}, received error {response.status_code} {response.text}")

    wrapper = response.json()
    ecs_path = wrapper['ecs_path']

    return path_prefix_to_add + ecs_path

def pathchange_datasets(target_db: MenasDb, collection_name: str, dataset_names_list: List[str],
                        mapping_svc_url: str, mapping_prefix: str, dryrun:bool) -> None:
    if not dataset_names_list:
        print("No datasets to path-change in {}, skipping.".format(collection_name))
        return

    print("Path changing of collection {} started".format(collection_name))
    dataset_collection = target_db.mongodb[collection_name]

    query = {"name": {"$in": dataset_names_list}}  # dataset name

    docs_count = dataset_collection.count_documents(query)
    docs = dataset_collection.find(query)

    print("Found: {} documents for the path change. In progress ... ".format(docs_count))

    patched = 0
    failed_count = 0
    for item in docs:
        # item preview
        if verbose:
            print("Changing paths for dataset {} v{} (_id={}).".format(item["name"], item["version"], item["_id"]))

        hdfs_path = item["hdfsPath"]
        hdfs_publish_path = item["hdfsPublishPath"]

        updated_hdfs_path = map_path_from_svc(hdfs_path, mapping_prefix, mapping_svc_url,)
        updated_hdfs_publish_path = map_path_from_svc(hdfs_publish_path, mapping_prefix, mapping_svc_url)

        if dryrun:
            print("  *would set* hdfsPath: {} -> {}".format(hdfs_path,  updated_hdfs_path))
            print("  *would set* hdfsPublishPath: {} -> {}".format(hdfs_publish_path,  updated_hdfs_publish_path))
            print("")

        else:
            try:
                if verbose:
                    print("  *changing* hdfsPath: {} -> {}".format(hdfs_path,  updated_hdfs_path))
                    print("  *changing* hdfsPublishPath: {} -> {}".format(hdfs_publish_path,  updated_hdfs_publish_path))

                update_result = dataset_collection.update_one(
                    {"_id": item["_id"]},
                    {"$set": {
                        "hdfsPath": updated_hdfs_path,
                        "hdfsPublishPath": updated_hdfs_publish_path,
                        "bakHdfsPath": hdfs_path,
                        "bakHdfsPublishPath": hdfs_publish_path
                        # todo add migration tag to properties map
                    }}
                )
                if update_result.acknowledged:
                    if verbose:
                        print("Successfully changed path for dataset {} v{} (_id={}).".format(item["name"], item["version"], item["_id"]))
                        print("")

            except Exception as e:
                print("Warning: Error while changing paths for dataset {} v{} (_id={}): {}".format(item["name"], item["version"], item["_id"], e))
                failed_count += 1
            else:
                patched += 1

    print("Successfully migrated {} of {} entries, failed: {}".format(patched, docs_count, failed_count))


def pathchange_collections_by_ds_names(target_db: MenasDb,
                                       supplied_ds_names: List[str],
                                       mapping_svc_url: str,
                                       mapping_prefix: str,
                                       dryrun: bool) -> None:

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names_found = target_db.get_distinct_ds_names_from_ds_names(supplied_ds_names, migration_free_only=False)
    print('Dataset names to path change (actually found db): {}'.format(ds_names_found))


    print("")
    pathchange_datasets(target_db, DATASET_COLLECTION, ds_names_found, mapping_svc_url, mapping_prefix, dryrun)

def run(parsed_args: argparse.Namespace):
    target_conn_string = parsed_args.target
    target_db_name = parsed_args.targetdb

    dryrun = args.dryrun  # if set, only path change description will be printed, no actual patching will run
    mapping_service = args.mappingservice
    mapping_prefix = args.mappingprefix

    print('Menas mongo ECS paths mapping')
    print('Running with settings: dryrun={}, verbose={}'.format(dryrun, verbose))
    print("Using mapping service at: {}".format(mapping_service))
    print('  target connection-string: {}'.format(target_conn_string))
    print('  target DB: {}'.format(target_db_name))

    target_db = MenasDb.from_connection_string(target_conn_string, target_db_name, alias="target db", verbose=verbose)

    # todo could be used for real menas
    # Checks raise MenasDbErrors
    # print("Checking target db validity...")
    # target_db.check_db_version()
    # target_db.check_menas_collections_exist()

    dataset_names = parsed_args.datasets

    # debug # todo remove
    # print("res" + map_path_from_svc("/bigdatahdfs/datalake/publish/dm9/CNSMR_ACCNT/country_code=KEN", mapping_service))

    # todo do remapping for mapping tables, too
    # mt_names = parsed_args.mtables

    pathchange_collections_by_ds_names(target_db, dataset_names, mapping_service, mapping_prefix, dryrun=dryrun)

    print("Done.")


if __name__ == '__main__':
    args = parse_args()

    # globals script vars
    verbose = args.verbose
    run(args)

    # example test-runs:
    # python dataset_paths_to_ecs.py mongodb://localhost:27017/admin -v -d DM9_actn_Cd -t menas_remap_test
