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
    'lock_migrated': True,
    'target_db_name': "menas_migrated"
})

migration_hash = secrets.token_hex(3)  # e.g. 34d4e10f

SOURCE_DB_NAME = "menas"

# Constants
NOT_LOCKED_MONGO_FILTER = {"$or": [
    {"locked": False},  # is not locked, or
    {"locked": {"$exists": False}}  # or: there is no locking info at all
]}



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
    parser.add_argument('-l', '--locking', action='store_true', default=defaults.lock_migrated,
                        help="locking of migrated entities")

    # todo target db name param
    parser.add_argument('source', metavar="SOURCE",
                        help="connection string for source MongoDb")
    parser.add_argument('target', metavar="TARGET",
                        help="connection string for target MongoDb")

    input_options_group = parser.add_mutually_exclusive_group(required=True)
    input_options_group.add_argument('-d', '--datasets', dest='datasets', metavar="DATASET_NAME", default=[],
                                     nargs="+", help='list datasets to migrate')
    input_options_group.add_argument('-m', '--mapping-tables', dest="mtables", metavar="MTABLE_NAME", default=[],
                                     nargs="+", help='list datasets to migrate')

    return parser.parse_args()


def get_database(conn_str, db_name):
    """
    Gets db handle
    :param db_name: string db name
    :param conn_str: connection string, e.g. mongodb://username1:password213@my.domain.ext/adminOrAnotherDb"
    :return: mongoDb handle
    """
    client = MongoClient(conn_str)
    majority_write_concern = WriteConcern(w="majority")
    majority_read_concern = ReadConcern(level="majority")

    return Database(client, db_name, write_concern=majority_write_concern, read_concern=majority_read_concern)


def get_distinct_ds_names_from_ds_names(db, ds_names):
    return get_distinct_entities_ids(db, ds_names, "dataset_v1")


def get_distinct_schema_names_from_schema_names(db, schema_names):
    return get_distinct_entities_ids(db, schema_names, "schema_v1")


def get_distinct_entities_ids(db, entity_names, collection_name, entity_name_field="name", distinct_field="name"):
    """ General way to retrieve distinct entity field values (names, ids, ...) from non-locked entities """
    collection = db[collection_name]

    entities = collection.distinct(
        distinct_field,  # field to distinct on
        {"$and": [
            {entity_name_field: {"$in": entity_names}},  # filter on name (ds/mt)
            NOT_LOCKED_MONGO_FILTER
        ]}
    )
    return entities  # array of distinct names (in a single document)


def get_distinct_mapping_tables_from_ds_names(db, ds_names):
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


def assemble_nonlocked_runs_from_ds_names(db, ds_names):
    return get_distinct_entities_ids(db, ds_names, "run_v1", entity_name_field="dataset", distinct_field="uniqueId")


def assemble_notlocked_schemas_from_ds_names(db, ds_names):
    return assemble_notlocked_schemas_from_x(db, ds_names, "dataset_v1", "schemaName")


def assemble_notlocked_schemas_from_mt_names(db, mt_names):
    return assemble_notlocked_schemas_from_x(db, mt_names, "mapping_table_v1", "schemaName")


def assemble_notlocked_schemas_from_x(db, entity_names, collection_name, distinct_field):
    # schema names from not locked X (datasets/mts) (the schemas themselves may or may not be locked)
    schema_names = get_distinct_entities_ids(db, entity_names, collection_name, distinct_field=distinct_field)

    # check schema collection which of these schemas are actually not locked:
    return get_distinct_schema_names_from_schema_names(db, schema_names)


def assemble_nonlocked_mapping_tables_from_mt_names(db, mt_names):
    return get_distinct_entities_ids(db, mt_names, "mapping_table_v1")


def assemble_nonlocked_mapping_tables_from_ds_names(db, ds_names):
    # mt names from not datasets (the mts themselves may or may not be locked)
    mt_names_from_ds_names = get_distinct_mapping_tables_from_ds_names(db, ds_names)

    return get_distinct_entities_ids(db, mt_names_from_ds_names, "mapping_table_v1")


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

    # This relies on the locking-update being complete on mongo-cluster, thus using majority r/w concerns.
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


def migrate_collections_by_ds_names(source, target, supplied_ds_names):
    source_db = get_database(source, SOURCE_DB_NAME)
    target_db = get_database(target, defaults.target_db_name)  # todo configurable?

    if verbose:
        print("Dataset names given: {}".format(supplied_ds_names))

    ds_names = get_distinct_ds_names_from_ds_names(source_db, supplied_ds_names)
    print('Dataset names to migrate: {}'.format(ds_names))

    ds_schema_names = assemble_notlocked_schemas_from_ds_names(source_db, ds_names)
    print('DS schemas to migrate: {}'.format(ds_schema_names))

    mapping_table_names = assemble_nonlocked_mapping_tables_from_ds_names(source_db, ds_names)
    print('MTs to migrate: {}'.format(mapping_table_names))

    mt_schema_names = assemble_notlocked_schemas_from_mt_names(source_db, mapping_table_names)
    print('MT schemas to migrate: {}'.format(mt_schema_names))

    run_unique_ids = assemble_nonlocked_runs_from_ds_names(source_db, ds_names)
    print('Runs to migrate: {}'.format(run_unique_ids))

    all_schemas = list(set.union(set(ds_schema_names), set(mt_schema_names)))
    if verbose:
        print('All schemas (DS & MT) to migrate: {}'.format(all_schemas))

    print("\n")
    migrate_entities(source_db, target_db, "schema_v1", all_schemas, describe_default_entity, entity_name="schema")
    migrate_entities(source_db, target_db, "dataset_v1", ds_names, describe_default_entity, entity_name="dataset")
    migrate_entities(source_db, target_db, "mapping_table_v1", mapping_table_names, describe_default_entity, entity_name="mapping table")
    migrate_entities(source_db, target_db, "run_v1", run_unique_ids, describe_run_entity, entity_name="run", name_field="uniqueId")
    # todo migrate attachments, too?


def migrate_collections_by_mt_names(source, target, supplied_mt_names):
    source_db = get_database(source, SOURCE_DB_NAME)
    target_db = get_database(target, defaults.target_db_name)  # todo configurable?

    if verbose:
        print("MT names given: {}".format(supplied_mt_names))

    mapping_table_names = assemble_nonlocked_mapping_tables_from_mt_names(source_db, supplied_mt_names)
    print('MTs to migrate: {}'.format(mapping_table_names))

    mt_schema_names = assemble_notlocked_schemas_from_mt_names(source_db, mapping_table_names)
    print('MT schemas to migrate: {}'.format(mt_schema_names))

    # todo is this all for my MT migration or should we reversly lookup datasets that use these MTs, their runs and ds schemas, too?

    print("\n")
    migrate_entities(source_db, target_db, "schema_v1", mt_schema_names, describe_default_entity, entity_name="schema")
    migrate_entities(source_db, target_db, "mapping_table_v1", mapping_table_names, describe_default_entity, entity_name="mapping table")
    # todo migrate attachments, too?


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

    dataset_names = args.datasets
    mt_names = args.mtables
    if dataset_names:
        print('dataset names supplied: {}'.format(dataset_names))
        migrate_collections_by_ds_names(source, target, dataset_names)
    elif mt_names:
        print('mapping table names supplied: {}'.format(mt_names))
        migrate_collections_by_mt_names(source, target, mt_names)
    else:
        # should not happen (-d/-m is exclusive and required)
        raise Exception("Invalid run options: DS names (-d ds1 ds2 ...).. or MT names (-m mt1 mt2 ... ) must be given.")

    print("Done.")

    # example test-runs:
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -v -d mydataset1 test654
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -d mydataset1 test654 Cobol1 Cobol2
    # migrate_menas.py mongodb://localhost:27017/admin mongodb://localhost:27017/admin -v -m MyAwesomeMappingTable1
