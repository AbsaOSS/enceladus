#!/usr/bin/python3

import argparse
from minydra.dict import MinyDict # dictionary with dot access
import pymongo

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
        formatter_class=argparse.ArgumentDefaultsHelpFormatter #prints default values, too, on help (-h)
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

    print('test: {}'.format(defaults.lockMigrated))

    print("Done.")
