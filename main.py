import pandas as pd
import argparse
import os
import logging

from helpers import read_yaml, response_message, upload_dataframe_to_bigquery


def main(text_file):
    config = read_yaml('config/package_vars.yml')

    try:
        df = load_file_into_dataframe(text_file)
    except ValueError as ve:
        return response_message(True, 422, ve)

    upload_dataframe_to_bigquery(df, config)
    return response_message(False, 200, 'Successfully uploaded data to BigQuery')


def load_file_into_dataframe(file):
    # extract the file name and extension
    file_name, file_extension = os.path.splitext(file)

    if file_extension == '.csv':
        return pd.read_csv(file)  # TODO: Test encoding and add config variables
    else:
        logging.warning(f'File type {file_extension} not supported yet')
        raise ValueError('File type not supported')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("text_file", type=str, help="location of text file to load")
    args = parser.parse_args()

    main(args.text_file)
