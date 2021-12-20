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

asdf = True


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


def assemble_ds_schemas(db, ds_names):
    ds_collection = db["dataset_v1"]

    schemas = ds_collection.distinct(
        "schemaName",  # field to distinct on
        {"name": {"$in": ds_names}}  # filter
    )
    return schemas  # array of distinct schemaNames (in a single document)


def assemble_ds_mapping_tables(db, ds_names):
    ds_collection = db["dataset_v1"]

    mapping_table_names = ds_collection.aggregate([
        {"$match": {"$and": [  # selection based on:
            {"name": {"$in": ds_names}},  # dataset name
            {"conformance": {"$elemMatch": {"_t": "MappingConformanceRule"}}}  # having some MCRs
        ]}},
        {"$unwind": "$conformance"},  # explodes each doc into multiple - each having single conformance rule
        {"$match": {"conformance._t": "MappingConformanceRule"}},  # filtering only MCRs, other CR are irrelevant
        {"$group": {"_id": "$conformance.mappingTable"}}  # essentially distinct on the field
    ])

    return mapping_table_names  # documents, each {_id : mappingTableNameThatIsDistinctOnTheName}


def get_migration_data(src, ds_names):
    db = get_database(src, "menas")

    schema_names = assemble_ds_schemas(db, ds_names)
    print('Schemas to migrate: {}'.format(schema_names))

    mapping_tables = assemble_ds_mapping_tables(db, ds_names)
    print('MTs to migrate:') # todo toArray?
    for mt in mapping_tables:
        print(mt)

    # todo schema for MT


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

    dsnames = ["mydataset1", "Cobol2", "test654", "toDelete1"]

    # just testing for now:
    get_migration_data(source, dsnames)

    print("Done.")

    # example test-run:
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin
