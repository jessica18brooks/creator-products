import yaml
import logging

import google.auth
from google.cloud import bigquery


def response_message(error, code, message):
    if error:
        return {'error': {'code': code, 'message': message}}
    else:
        return {'success': {'code': code, 'message': message}}


def read_yaml(file):
    try:
        with open(file, 'r') as f:
            return yaml.safe_load(f)['variables']
    except OSError:
        logging.error(f'Failed to load yaml file {file}')
    except KeyError:
        logging.error(f'No "variables" defined in package_vars')


def upload_dataframe_to_bigquery(dataframe, variables):
    bq_client = bigquery.Client()

    try:
        project_id = variables['project_id']
    except KeyError:
        _, project_id = google.auth.default()

    logging.debug(f'Loading to project {project_id}')

    try:
        dataset_ref = bigquery.DatasetReference(project_id, variables['dataset_id'])
    except KeyError:
        logging.error(f'No dataset_id defined in package_vars')
        return response_message(True, 400, 'No dataset_id defined in package_vars')

    try:
        table_ref = dataset_ref.table(variables['table_id'])
    except KeyError:
        logging.error(f'No table_id defined in package_vars')
        return response_message(True, 400, 'No table_id defined in package_vars')

    job_config = bigquery.LoadJobConfig()  # TODO: Set up schema for load job

    job = bq_client.load_table_from_dataframe(
        dataframe, table_ref, job_config=job_config
    )
    job.result()  # Wait for the job to complete.

    table = bq_client.get_table(table_ref)
    logging.info(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_ref}')
