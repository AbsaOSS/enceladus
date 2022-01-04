#!/usr/bin/python3

import argparse
import secrets  # migration hash generation
from minydra.dict import MinyDict  # dictionary with dot access
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern


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

    ## return client[db_name]  # gets or creates db
    majority_write_concern = WriteConcern(w="majority")
    majority_read_concern = ReadConcern(level="majority")

    return Database(client, db_name, write_concern=majority_write_concern, read_concern=majority_read_concern)


NOT_LOCKED_MONGO_FILTER = {"$or": [
    {"locked": False},  # is not locked, or
    {"locked": {"$exists": False}}  # or: there is no locking info at all
]}


def assemble_schemas(db, entity_names, collection_name):
    return assemble_default_entities(db, entity_names, collection_name, distinct_field="schemaName")


def assemble_datasets(db, ds_names):
    return assemble_default_entities(db, ds_names, "dataset_v1", distinct_field="name")


def assemble_default_entities(db, entity_names, collection_name, distinct_field):
    """ Assembles schemas or datasets based on entity names and not being locked """
    collection = db[collection_name]

    entities = collection.distinct(
        distinct_field,  # field to distinct on
        {"$and": [
            {"name": {"$in": entity_names}},  # filter on name (ds/mt)
            NOT_LOCKED_MONGO_FILTER
        ]}
    )
    return entities  # array of distinct names (in a single document)


def assemble_runs(db, ds_names):
    collection = db["run_v1"]

    ids = collection.distinct(
        "uniqueId",  # field to distinct on
        {"$and": [
            {"dataset": {"$in": ds_names}},  # filter on DS names
            NOT_LOCKED_MONGO_FILTER
        ]}
    )
    return ids  # array of distinct uniqueId (in a single document)


def assemble_ds_schemas(db, ds_names):
    """Since schemas are assembled from datasets, only locked state of the datasets is checked.
    Locked state of the schemas is not checked here. However, later, in the migration phase, only non-locked schemas
    are actually migrated."""
    return assemble_schemas(db, ds_names, "dataset_v1")


def assemble_mt_schemas(db, mt_names):
    return assemble_schemas(db, mt_names, "mapping_table_v1")


def assemble_ds_mapping_tables(db, ds_names):
    ds_collection = db["dataset_v1"]

    mapping_table_names = ds_collection.aggregate([
        {"$match": {"$and": [  # selection based on:
            {"name": {"$in": ds_names}},  # dataset name
            {"conformance": {"$elemMatch": {"_t": "MappingConformanceRule"}}},  # having some MCRs
            NOT_LOCKED_MONGO_FILTER
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


def migrate_collections(source, target, supplied_ds_names):
    source_db = get_database(source, "menas")
    target_db = get_database(target, 'menas_migrated')

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names = assemble_datasets(source_db, supplied_ds_names)
    print('Dataset names to migrate: {}'.format(ds_names))

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

    print("\n")
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
        {"$and": [
            {name_field: {"$in": entity_names_list}},  # dataset/schema/mt name or run uniqueId
            NOT_LOCKED_MONGO_FILTER
        ]},
        {"$set": {
            "migrationHash": migration_hash,
            "locked": True
        }}
    )

    if update_result.acknowledged:
        if verbose:
            print("Successfully locked {}s: {}. Migrating ... ".format(entity_name, update_result.modified_count))
    else:
        raise Exception("Locking unsuccessful: {}, matched={}, modified={}"
                        .format(update_result.acknowledged, update_result.matched_count, update_result.modified_count))

    # This relies on the locking-update being completed, this is why we are using majority r/w concerns.
    # https://docs.mongodb.com/manual/core/causal-consistency-read-write-concerns/#std-label-causal-rc-majority-wc-majority
    docs = dataset_collection.find(
        {"$and": [
            {name_field: {"$in": entity_names_list}},  # dataset name
            {"migrationHash": migration_hash},  # belongs to this migration
            {"locked": True}  # is successfully locked (previous step)
        ]}
    )

    target_dataset_collection = target_db[collection_name + "migrated"]  # todo make configurable
    migrated_count = 0
    for item in docs:
        # item preview
        if verbose:
            print("Migrating {}: {}.".format(entity_name, describe_fn(item)))
        del item["locked"]  # the original is locked, but the migrated in target should not be (keeping the migration #)
        target_dataset_collection.insert_one(item)
        migrated_count += 1

    locked_count = update_result.modified_count
    if locked_count != migrated_count:
        raise Exception("Locked {} {}s, but managed to migrate only {} of them!"
                        .format(locked_count, entity_name, migrated_count))

    print("Migration of collection {} finished, migrated {} {}s\n".format(collection_name, migrated_count, entity_name))


def describe_default_entity(item):
    """
    Aux method to describe dataset/schema/mapping-table object - relying on fields 'name' and 'version' being present
    :param item: object to describe
    :return: formatted description string
    """
    return "{} v{}".format(item["name"], item["version"])


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

    dsnames = ["mydataset1" ] #, "Cobol2", "test654", "toDelete1"]
    migrate_collections(source, target, dsnames)

    print("Done.")

    # example test-run:
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -v
