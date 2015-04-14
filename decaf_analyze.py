import fnmatch
import os
import shutil
import sys
import yaml

__author__ = 'jfogarty'


def decaf_num_records(dir_yml: str):
    # This will hold counts by record type
    record_counts = {}
    """:type : dict of [str, int]"""

    # Go through all our files
    for file in os.listdir(dir_yml):
        if fnmatch.fnmatch(file, '*.yml'):
            print('Processing {}'.format(file), file=sys.stderr)
            with open(os.path.join(dir_yml, file)) as f:
                # Load the file
                yml = yaml.load(f)

            # Root of the file is the type, as a dictionary key, only one item
            type_record = list(yml.keys())[0]
            records = yml[type_record]

            # Add to our count
            record_counts[type_record] = record_counts.get(type_record, 0) + len(records)

    print('Number of Records:')
    print(dir_yml)
    for type_record in sorted(record_counts.keys()):
        print('  {:6d} {}'.format(record_counts[type_record], type_record))
    print('')


def decaf_num_records_per_user(dir_yml: str):
    user_record_counts = {}
    """:type : dict of [str, dict of [str, int]]"""

    # Create our user_id keys
    for file in os.listdir(dir_yml):
        if fnmatch.fnmatch(file, 'user_*.yml'):
            with open(os.path.join(dir_yml, file)) as f:
                # Load the file
                yml = yaml.load(f)

            # Root of the file is the type, as a dictionary key, only one item
            type_record = list(yml.keys())[0]
            records = yml[type_record]

            # Find the user_id value, create a new dictionary
            for record_current in records:
                user_record_counts[record_current['user_id']] = {}

    # Go over our records, counting by user_id
    for file in os.listdir(dir_yml):
        if fnmatch.fnmatch(file, '*.yml'):
            print('Processing {}'.format(file), file=sys.stderr)
            with open(os.path.join(dir_yml, file)) as f:
                # Load the file
                yml = yaml.load(f)

            # Root of the file is the type, as a dictionary key, only one item
            type_record = list(yml.keys())[0]
            records = yml[type_record]

            # Check the user_id on each record
            for record_current in records:
                if record_current['user_id'] in user_record_counts:
                    record_counts = user_record_counts[record_current['user_id']]
                    record_counts[type_record] = record_counts.get(type_record, 0) + 1

    print('Number of Records Per User:')
    print(dir_yml)
    for user_id in sorted(user_record_counts.keys()):
        print('user_id: {}'.format(user_id))
        record_counts = user_record_counts[user_id]
        for type_record in sorted(record_counts.keys()):
            print('  {:6d} {}'.format(record_counts[type_record], type_record))
    print('')


def decaf_data_per_user(dir_yml: str, user_id: int):
    # Remove any prior execution, make our directory
    dir_user_yml = os.path.join(dir_yml, str(user_id))
    if os.path.exists(dir_user_yml):
        shutil.rmtree(dir_user_yml)
    os.mkdir(dir_user_yml)

    # Go over our records, save out records related to this user_id
    for file in os.listdir(dir_yml):
        if fnmatch.fnmatch(file, '*.yml'):
            print('Processing {}'.format(file), file=sys.stderr)
            with open(os.path.join(dir_yml, file)) as f:
                # Load the file
                yml = yaml.load(f)

            # Root of the file is the type, as a dictionary key, only one item
            type_record = list(yml.keys())[0]

            # Keep the matching IDs
            yml[type_record] = [record for record in yml[type_record] if record['user_id'] == user_id]

            # Only store if we actually have some matching records
            if yml[type_record]:
                with open(os.path.join(dir_user_yml, file), 'w') as f:
                    # Write to the file
                    yaml.dump(
                        yml,
                        stream=f,
                        default_flow_style=False
                    )


if __name__ == '__main__':
    decaf_num_records('secrets/decaf_dump_results/dump_small')
    decaf_num_records_per_user('secrets/decaf_dump_results/dump_small')

    decaf_data_per_user('secrets/decaf_dump_results/dump_small', 1)
    decaf_num_records('secrets/decaf_dump_results/dump_small/1')
    decaf_data_per_user('secrets/decaf_dump_results/dump_small', 2)
    decaf_num_records('secrets/decaf_dump_results/dump_small/2')

    decaf_num_records_per_user('secrets/decaf_dump_results/dump_large')

    decaf_data_per_user('secrets/decaf_dump_results/dump_large', 3)
    decaf_data_per_user('secrets/decaf_dump_results/dump_large', 33)
