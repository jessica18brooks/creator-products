import pandas as pd
import argparse
import os
import logging

import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
from helpers import read_yaml, response_message, upload_dataframe_to_bigquery, create_aggregated_table

logging.basicConfig(level=logging.DEBUG)


def main(text_file):
    config = read_yaml('config/package_vars.yml')

    try:
        df = load_file_into_dataframe(text_file)
    except ValueError as ve:
        logging.error(response_message(True, 422, ve))
        return response_message(True, 422, ve)

    project_id = get_project_id(config)
    bq_client = create_bq_client(config['key_file'])

    upload_dataframe_to_bigquery(project_id, bq_client, df, config)

    create_aggregated_table(project_id, bq_client, config)
    return response_message(False, 200, 'Successfully uploaded data to BigQuery')


def load_file_into_dataframe(file):
    # extract the file name and extension
    file_name, file_extension = os.path.splitext(file)
    logging.debug(f'this directory: {__file__}, file_name: {file_name}, file_extension: {file_extension}')

    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    file_ref = os.path.join(__location__, file)

    if file_extension == '.csv':
        return pd.read_csv(file_ref, encoding='cp437')
    else:
        logging.warning(f'File type {file_extension} not supported yet')
        raise ValueError('File type not supported')


def get_project_id(variables):
    try:
        project_id = variables['project_id']
    except KeyError:
        _, project_id = google.auth.default()

    logging.debug(f'Running in project {project_id}')
    return project_id


def create_bq_client(key_file):
    credentials = service_account.Credentials.from_service_account_file(
        key_file, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    return bigquery.Client(credentials=credentials, project=credentials.project_id)


if __name__ == '__main__':
    logging.debug(f'Begin run')
    parser = argparse.ArgumentParser()
    parser.add_argument("text_file", type=str, help="location of text file to load")
    args = parser.parse_args()

    main(args.text_file)
