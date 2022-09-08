import json
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


def generate_schema(json_data):
    schema_array = []
    for s in json_data:
        schema_array.append(bigquery.SchemaField(s['name'], s['type'], mode=s['mode']))

    return schema_array


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

    try:
        with open('config/SampleDataSchema.json', 'r') as j:
            schema = generate_schema(json.load(j))
    except OSError:
        logging.error(f'Failed to open json schema')
        return response_message(True, 400, 'Failed to open json schema')

    try:
        table = bq_client.create_table(bigquery.Table(table_ref, schema), exists_ok=True)
    except Exception as e:
        logging.error(f'Failed to create BigQuery table {table_ref} with error {e.args}')
        return response_message(True, 400, 'Failed to create BigQuery table')

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = ','
    job_config.schema = schema
    job_config.autodetect = False
    job_config.skip_leading_rows = 1
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job = bq_client.load_table_from_dataframe(
        dataframe, table, job_config=job_config
    )
    job.result()  # Wait for the job to complete.

    table = bq_client.get_table(table_ref)
    logging.info(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_ref}')
