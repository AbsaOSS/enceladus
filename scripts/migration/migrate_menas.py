#!/usr/bin/python3

import argparse
from minydra.dict import MinyDict  # dictionary with dot access
from pymongo import MongoClient

# Default configuration
# =====================

defaults = MinyDict({
    'verbose': False,
    'dryrun': False,
    'lockMigrated': True
})

migration_hash = "34df"  # todo generate at some point


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


def simple_migration_test(src, tgt):
    """
    First ugly simplistic migration attempt
    """
    src_db = get_database(src, 'menas')  # todo make configurable

    dataset_collection = src_db["dataset_v1"]
    docs = dataset_collection.find()
    just_some_docs = docs[:2]

    target_db = get_database(tgt, 'migrated')  # todo change # would create db with default settings
    target_dataset_collection = target_db["dataset_v1b"]  # would create coll. with default settings (no extra indices!)

    for item in just_some_docs:
        # item preview
        print(item)
        target_dataset_collection.insert_one(item)


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
    if not list(mapping_table_names):
        return []

    extracted_array = list(mapping_table_names)[0]['mts']
    return extracted_array


def migrate_collections(source, target, ds_names):
    source_db = get_database(source, "menas")
    target_db = get_database(target, 'menas_migrated')

    ds_schema_names = assemble_ds_schemas(source_db, ds_names)
    print('DS schemas to migrate: {}'.format(ds_schema_names))

    mapping_table_names = assemble_ds_mapping_tables(source_db, ds_names)
    print('MTs to migrate: {}'.format(mapping_table_names))

    mt_schema_names = assemble_mt_schemas(source_db, mapping_table_names)
    print('MT schemas to migrate: {}'.format(mt_schema_names))

    runs = assemble_runs(source_db, ds_names)
    print('Runs to migrate: {}'.format(runs))

    migrate_schemas(source_db, target_db, ds_schema_names)
    # todo migrate the rest of the entities and generalize


def migrate_schemas(source_db, target_db, ds_schema_names):
    print("migrating schemas started")

    dataset_collection = source_db["schema_v1"]

    # mark as locked first
    update_result = dataset_collection.update_many(
        {"name": {"$in": ds_schema_names}},  # dataset name
        {"$set": {
            "migrationHash": migration_hash,
            "locked": True
        }}
    )

    if update_result.acknowledged:
        if verbose:
            print("Locking of {} successful. Migrating ... ".format(update_result.modified_count))
    else:
        raise Exception("Locking unsuccessful: {}, matched={}, modified={}"
                        .format(update_result.acknowledged, update_result.matched_count, update_result.modified_count))

    docs = dataset_collection.find(
        {"$and": [
            {"name": {"$in": ds_schema_names}},  # dataset name
            {"migrationHash": migration_hash},  # belongs to this migration
            {"locked": True}  # is successfully locked (previous step)
        ]}
    )

    target_dataset_collection = target_db["schema_v1migrated"]  # todo make configurable
    for item in docs:
        # item preview
        if verbose:
            print("Migrating schema: {} v{}.".format(item["name"], item["version"]))
        del item["locked"]  # the original is locked, but the migrated in target should not be (keeping the migration #)
        target_dataset_collection.insert_one(item)

    print("Migrating schemas finished.")


if __name__ == '__main__':
    args = parse_args()

    dryrun = args.dryrun
    verbose = args.verbose
    locking = args.locking

    source = args.source
    target = args.target

    print('Menas mongo migration')
    print('running with settings: dryrun={}, verbose={}, locking={}'.format(dryrun, verbose, locking))
    print('  source: {}'.format(source))
    print('  target: {}'.format(target))

    # simple_migration_test(source, target)

    dsnames = ["mydataset1"]  # , "Cobol2", "test654", "toDelete1"

    # just testing for now:
    migrate_collections(source, target, dsnames)

    print("Done.")

    # example test-run:
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -v
