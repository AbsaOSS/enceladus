#!/usr/bin/python3

import argparse
import secrets  # migration hash generation
from minydra.dict import MinyDict  # dictionary with dot access
from pymongo import MongoClient


# Default configuration
# =====================

defaults = MinyDict({
    'verbose': False,
    'dryrun': False,
    'lockMigrated': True
})

migration_hash = secrets.token_hex(3)  # e.g. 34d4e10f


def parse_args():
    parser = argparse.ArgumentParser(
        prog='migrate_menas',
        description='Menas MongoDb migration script.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter  # prints default values, too, on help (-h)
        )

    parser.add_argument('-n', '--dryrun', action='store_true', default=defaults.dryrun,
                        help="if specified, skip the actual synchronization, just print what would be copied over.")
    parser.add_argument('-v', '--verbose', action="store_true", default=defaults.verbose,
                        help="prints extra information while running.")
    parser.add_argument('-l', '--locking', action='store_true', default=defaults.lockMigrated,
                        help="locking of migrated entities")

    parser.add_argument('source', metavar="SOURCE",
                        help="connection string for source MongoDb")
    parser.add_argument('target', metavar="TARGET",
                        help="connection string for target MongoDb")
    return parser.parse_args()


def get_database(conn_str, db_name):
    """
    Gets db handle
    :param db_name: string db name
    :param conn_str: connection string, e.g. mongodb://username1:password213@my.domain.ext/adminOrAnotherDb"
    :return: mongoDb handle
    """
    client = MongoClient(conn_str)

    return client[db_name]  # gets or creates db


def assemble_schemas_general(db, entity_names, collection_name):
    collection = db[collection_name]

    schemas = collection.distinct(
        "schemaName",  # field to distinct on
        {"name": {"$in": entity_names}}  # filter
    )
    return schemas  # array of distinct schemaNames (in a single document)


def assemble_runs(db, ds_names):
    collection = db["run_v1"]

    ids = collection.distinct(
        "uniqueId",  # field to distinct on
        {"dataset": {"$in": ds_names}}  # filter
    )
    return ids  # array of distinct uniqueId (in a single document)


def assemble_ds_schemas(db, ds_names):
    return assemble_schemas_general(db, ds_names, "dataset_v1")


def assemble_mt_schemas(db, mt_names):
    return assemble_schemas_general(db, mt_names, "mapping_table_v1")


def assemble_ds_mapping_tables(db, ds_names):
    ds_collection = db["dataset_v1"]

    mapping_table_names = ds_collection.aggregate([
        {"$match": {"$and": [  # selection based on:
            {"name": {"$in": ds_names}},  # dataset name
            {"conformance": {"$elemMatch": {"_t": "MappingConformanceRule"}}}  # having some MCRs
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

    extracted_array = mapping_table_names_list[0]['mts']
    return extracted_array


def migrate_collections(source, target, ds_names):
    source_db = get_database(source, "menas")
    target_db = get_database(target, 'menas_migrated')

    print('Datasets to migrate: {}'.format(ds_names))

    ds_schema_names = assemble_ds_schemas(source_db, ds_names)
    print('DS schemas to migrate: {}'.format(ds_schema_names))

    mapping_table_names = assemble_ds_mapping_tables(source_db, ds_names)
    print('MTs to migrate: {}'.format(mapping_table_names))

    mt_schema_names = assemble_mt_schemas(source_db, mapping_table_names)
    print('MT schemas to migrate: {}'.format(mt_schema_names))

    runUniqueIds = assemble_runs(source_db, ds_names)
    print('Runs to migrate: {}'.format(runUniqueIds))

    all_schemas = list(set.union(set(ds_schema_names), set(mt_schema_names)))
    if verbose:
        print('All schemas (DS & MT) to migrate: {}'.format(all_schemas))

    migrate_entities(source_db, target_db, "schema_v1", all_schemas, describe_default_entity, entity_name="schema")
    migrate_entities(source_db, target_db, "dataset_v1", ds_names, describe_default_entity, entity_name="dataset")
    migrate_entities(source_db, target_db, "mapping_table_v1", mapping_table_names, describe_default_entity, entity_name="mapping table")
    migrate_entities(source_db, target_db, "run_v1", runUniqueIds, describe_run_entity, entity_name="run", name_field="uniqueId")
    # todo migrate attachments, too?


def migrate_entities(source_db, target_db, collection_name, entity_names_list,
                     describe_fn, entity_name="entity", name_field="name"):
    if not entity_names_list:
        print("No {}s to migrate in {}, skipping.".format(entity_name, collection_name))
        return

    print("Migration of collection {} started".format(collection_name))

    dataset_collection = source_db[collection_name]

    # mark as locked first
    update_result = dataset_collection.update_many(
        {name_field: {"$in": entity_names_list}},  # dataset/schema/mt name or run uniqueId
        {"$set": {
            "migrationHash": migration_hash,
            "locked": True
        }}
    )

    if update_result.acknowledged:
        if verbose:
            print("Successfully locked entities: {}. Migrating ... ".format(update_result.modified_count))
    else:
        raise Exception("Locking unsuccessful: {}, matched={}, modified={}"
                        .format(update_result.acknowledged, update_result.matched_count, update_result.modified_count))

    docs = dataset_collection.find(
        {"$and": [
            {name_field: {"$in": entity_names_list}},  # dataset name
            {"migrationHash": migration_hash},  # belongs to this migration
            {"locked": True}  # is successfully locked (previous step)
        ]}
    )

    target_dataset_collection = target_db[collection_name + "migrated"]  # todo make configurable
    for item in docs:
        # item preview
        if verbose:
            print("Migrating {}: {}.".format(entity_name, describe_fn(item)))
        del item["locked"]  # the original is locked, but the migrated in target should not be (keeping the migration #)
        target_dataset_collection.insert_one(item)

    print("Migration of collection {} finished.".format(collection_name))


def describe_default_entity(item):
    """
    Aux method to describe dataset/schema/mapping-table object - relying on fields 'name' and 'version' being present
    :param item: object to describe
    :return: formatted description string
    """
    return "{} v{}.".format(item["name"], item["version"])


def describe_run_entity(item):
    """
    Aux method to describe run object - relying on fields 'dataset', 'datasetVersion', and 'uniqueId' being present
    :param item: object to describe
    :return: formatted description string
    """
    return "for {} v{} - run {} (uniqueId {})".format(item["dataset"], item["datasetVersion"], item["runId"], item["uniqueId"])


if __name__ == '__main__':
    args = parse_args()

    dryrun = args.dryrun
    verbose = args.verbose
    locking = args.locking

    source = args.source
    target = args.target

    print('Menas mongo migration')
    print('Running with settings: dryrun={}, verbose={}, locking={}'.format(dryrun, verbose, locking))
    print("Using migration #: '{}'".format(migration_hash))
    print('  source: {}'.format(source))
    print('  target: {}'.format(target))

    dsnames = ["mydataset1", "Cobol2", "test654", "toDelete1"]
    migrate_collections(source, target, dsnames)

    print("Done.")

    # example test-run:
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -v