#!/usr/bin/env python3


# import modules
from google.cloud import storage
import os
import re
import csv
import os.path


def cs_to_cs(request):

    storage_client = storage.Client("<project-id>")

    # Create a bucket object for our bucket
    bucket_name = '<source-bucket-name>'
    bucket = storage_client.get_bucket(bucket_name)
    # Create a blob object from the filepath
    blob = bucket.blob("<file-path-to-input-file>")

    blob.download_to_filename('/tmp/<input-file>')

    # Replace all of these subfunctions with your file transformation function(s)
    # Example below takes a SQL dump and converts it to CSV file(s)

    def is_insert(line):
        return 'INSERT INTO' in line or False

    def get_values(line):
        return line.partition(' VALUES ')[2]

    def get_table_name(line):
        match = re.search('INSERT INTO `([0-9_a-zA-Z]+)`', line)
        if match:
            return match.group(1)
        else:
            # There are a few cases where the query is malformed, these are handled in main()
            return None

    def values_sanity_check(values):
        assert values
        assert values[0] == '('
        # Assertions have not been raised
        return True

    def parse_values(values):
        rows = []
        latest_row = []

        reader = csv.reader([values], delimiter=',',
                            doublequote=False,
                            escapechar='\\',
                            quotechar="'",
                            strict=True
                            )

        for reader_row in reader:
            for column in reader_row:
                if len(column) == 0 or column == 'NULL':
                    latest_row.append(chr(0))
                    continue
                if column[0] == "(":
                    new_row = False
                    if len(latest_row) > 0:
                        if latest_row[-1][-1] == ")":
                            latest_row[-1] = latest_row[-1][:-1]
                            new_row = True
                    if new_row:
                        latest_row = ['' if field == '\x00' else field for field in latest_row]

                        rows.append(latest_row)
                        latest_row = []
                    if len(latest_row) == 0:
                        column = column[1:]
                latest_row.append(column)
            if latest_row[-1][-2:] == ");":
                latest_row[-1] = latest_row[-1][:-2]
                latest_row = ['' if field == '\x00' else field for field in latest_row]

                rows.append(latest_row)

            return rows

    def main(filepath, output_folder):
        core_tables = []
        processedcount, error_count = 0, 0
        with open(filepath, 'rb') as f:
            for line in f.readlines():
                try:
                    line = line.decode("utf-8")
                except UnicodeDecodeError:
                    line = str(line)

                if is_insert(line):
                    table_name = get_table_name(line)
                    if (table_name is None):
                        error_count += 1
                        continue
                    if (table_name not in core_tables):
                        continue

                    values = get_values(line)
                    if values_sanity_check(values):
                        rows = parse_values(values)

                    if not os.path.isfile(output_folder + table_name + '.csv'):
                        with open(output_folder + table_name + '.csv', 'w', newline='') as outcsv:
                            writer = csv.writer(outcsv, quoting=csv.QUOTE_ALL)
                            for row in rows:
                                try:
                                    writer.writerow(row)
                                    processedcount += 1
                                except Exception as e:
                                    error_count += 1
                    else:
                        with open(output_folder + table_name + '.csv', 'a', newline='') as outcsv:
                            writer = csv.writer(outcsv, quoting=csv.QUOTE_ALL)
                            for row in rows:
                                try:
                                    writer.writerow(row)
                                    processedcount += 1
                                except Exception as e:
                                    error_count += 1

        print('FINISHED. PROCESSED: ', processedcount, ' ERRORCOUNT: ', error_count)

# Upload converted files to a destination bucket
    def upload_blob(file_name):
        """Uploads a file to the bucket."""
        bucket_name = '<destination-bucket-name>'
        storage_client = storage.Client("<project-id>")
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_filename(file_name)

        print('BEGINNING <describe-the-transformation> EXPORT.')

    print("job completed")
    main('/tmp/<input-file>', '/tmp/')
