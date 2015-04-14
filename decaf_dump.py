import mysql.connector
import os
import os.path
import shutil
import yaml

__author__ = 'jayfo'


def dump_table(name, row_group_size, row_max):
    """
    Dump a given table into a dictionary appropriate to dump to YML.

    :param name: Name of the table to dump.
    :param row_group_size: Number of rows in each query / file.
    :param row_max: Maximum number of total rows.
    :return: Dictionary appropriate to dump in YML.
    """

    # Open a connection based on our config
    with open('secrets/config.yml') as f:
        secrets = yaml.load(f)
    connection = mysql.connector.connect(
        host=secrets['host'],
        user=secrets['user'],
        password=secrets['password'],
        database=secrets['database']
    )
    connection.connect()

    cursor = connection.cursor()
    query = 'SHOW COLUMNS FROM {}'.format(name)
    cursor.execute(query)
    list_columns = cursor.fetchall()
    cursor.close()

    # Do not accidentally get more rows than we want
    if row_group_size == 0 and row_max > 0:
        row_group_size = row_max

    # Do a series of queries
    count_retrieved = 0
    query_continue = True
    while query_continue:
        cursor = connection.cursor()

        if row_max > 0 and row_max - count_retrieved < row_group_size:
            row_group_size = row_max - count_retrieved

        if row_group_size > 0:
            query = 'SELECT * FROM {} LIMIT {} OFFSET {}'.format(name, row_group_size, count_retrieved)
        else:
            query = 'SELECT * FROM {}'.format(name, row_max)
            query_continue = False

        cursor.execute(query)

        list_rows = cursor.fetchall()

        if list_rows:
            yml_rows = []
            for row_current in list_rows:
                yml_row_current = {}
                for column_index, column_current in enumerate(list_columns):
                    yml_row_current[column_current[0]] = row_current[column_index]
                yml_rows.append(yml_row_current)

            count_retrieved += len(yml_rows)

            yield yml_rows

        # If we got all the rows this time, we are done
        query_continue = query_continue and row_group_size > 0
        if row_max > 0:
            query_continue = query_continue and count_retrieved < row_max
        # If we did not get any rows this time, we are done
        query_continue = query_continue and list_rows

    cursor.close()
    connection.close()


def dump_to_file(yml_table, path):
    """
    Dump a table appropriate for YML into a file.

    :param yml_table: The table to be dumped.
    :param path: The path of the file to write.
    :return:
    """

    print('Dumping {}'.format(path))
    with open(path, 'w') as f:
        yaml.dump(
            yml_table,
            stream=f,
            default_flow_style=False
        )


def decaf_dump():
    """
    Our main dump function.
    :return:
    """

    # Erase any prior results
    if os.path.exists('secrets/decaf_dump_results'):
        shutil.rmtree('secrets/decaf_dump_results')

    # Create our dump directories
    os.mkdir('secrets/decaf_dump_results')

    os.mkdir('secrets/decaf_dump_results/dump_small')
    os.mkdir('secrets/decaf_dump_results/dump_small/photos')
    os.mkdir('secrets/decaf_dump_results/dump_large')
    os.mkdir('secrets/decaf_dump_results/dump_large/photos')

    # Enumerate the configs we want to dump
    configs_dump = [
        {'name': 'user_raw', 'table': 'users', 'dir': '.', 'row_group_size': 0, 'row_max': 0},
        {'name': 'personal_info_raw', 'table': 'personal_info', 'dir': '.', 'row_group_size': 0, 'row_max': 0},

        {'name': 'user', 'table': 'users', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 0},
        {'name': 'personal_info', 'table': 'personal_info', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 0},
        {'name': 'accelerometer', 'table': 'accelerometer', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 100},
        {'name': 'calendar', 'table': 'calendar', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 100},
        {'name': 'location', 'table': 'locations', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 100},
        {'name': 'motion', 'table': 'motion', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 100},
        {'name': 'wifi', 'table': 'wifi', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 100},
        {'name': 'food', 'table': 'foodPhotos', 'dir': 'dump_small', 'row_group_size': 0, 'row_max': 100},

        {'name': 'user', 'table': 'users', 'dir': 'dump_large', 'row_group_size': 0, 'row_max': 0},
        {'name': 'personal_info', 'table': 'personal_info', 'dir': 'dump_large', 'row_group_size': 0, 'row_max': 0},
        {'name': 'accelerometer', 'table': 'accelerometer', 'dir': 'dump_large', 'row_group_size': 1000, 'row_max': 0},
        {'name': 'calendar', 'table': 'calendar', 'dir': 'dump_large', 'row_group_size': 1000, 'row_max': 0},
        {'name': 'location', 'table': 'locations', 'dir': 'dump_large', 'row_group_size': 1000, 'row_max': 0},
        {'name': 'motion', 'table': 'motion', 'dir': 'dump_large', 'row_group_size': 1000, 'row_max': 0},
        {'name': 'wifi', 'table': 'wifi', 'dir': 'dump_large', 'row_group_size': 1000, 'row_max': 0},
        {'name': 'food', 'table': 'foodPhotos', 'dir': 'dump_large', 'row_group_size': 200, 'row_max': 0},
    ]

    # Iterate over each dump configuration
    for config_current in configs_dump:
        generator_rows = dump_table(
            config_current['table'],
            row_group_size=config_current['row_group_size'],
            row_max=config_current['row_max']
        )
        # Iterate over chunks of rows
        for file_count, yml_rows in enumerate(generator_rows):
            # Iterate over the row, looking for anything we need to clean up
            for yml_row_current in yml_rows:
                if config_current['name'] == 'user':
                    # Strip out these fields
                    if 'email' in yml_row_current:
                        del yml_row_current['email']
                elif config_current['name'] == 'personal_info':
                    # Strip out these fields
                    if 'email' in yml_row_current:
                        del yml_row_current['email']
                    if 'phoneNumber' in yml_row_current:
                        del yml_row_current['phoneNumber']
                elif config_current['name'] == 'food':
                    # Strip out this field, it's huge and weirdly encoded
                    if 'foursquare_values' in yml_row_current:
                        del yml_row_current['foursquare_values']
                    # Get the photo path pointing somewhere useful
                    path_photo = yml_row_current.get('photoPath', None)
                    if path_photo is not None:
                        path_photo_server = path_photo.replace(
                            '../files/foodPhotos/',
                            'secrets/server_dump/server_photos/'
                        )
                        path_photo_dump = path_photo.replace(
                            '../files/foodPhotos/',
                            'photos/'
                        )
                        shutil.copyfile(
                            path_photo_server,
                            'secrets/decaf_dump_results/{}/{}'.format(config_current['dir'], path_photo_dump)
                        )
                        yml_row_current['photoPath'] = path_photo_dump

            # Write out the chunk of rows
            yml_dump = {config_current['name']: yml_rows}
            dump_to_file(
                yml_dump,
                'secrets/decaf_dump_results/{}/{}_{:04d}.yml'.format(
                    config_current['dir'],
                    config_current['name'],
                    file_count
                )
            )


if __name__ == '__main__':
    decaf_dump()
