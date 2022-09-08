import json
import yaml
import logging

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


def upload_dataframe_to_bigquery(project_id, bq_client, dataframe, variables):

    try:
        dataset_ref = bigquery.DatasetReference(project_id, variables['dataset_id'])
    except KeyError:
        logging.error(f'No dataset_id defined in package_vars')
        return response_message(True, 400, 'No dataset_id defined in package_vars')

    try:
        bq_client.create_dataset(dataset_ref, exists_ok=True)
    except Exception as e:
        logging.error(f'Failed to create BigQuery dataset {dataset_ref} with error {e.args}')
        return response_message(True, 400, 'Failed to create BigQuery dataset')

    try:
        table_ref = dataset_ref.table(variables['source_table_id'])
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

    try:
        job = bq_client.load_table_from_dataframe(
            dataframe, table, job_config=job_config
        )
        job.result()  # Wait for the job to complete.
    except ValueError as ve:
        logging.error(f'Failed to load Dataframe into BigQuery table {table_ref} with error {ve.args}')
        return response_message(True, 400, 'Failed to load data into BigQuery table')

    table = bq_client.get_table(table_ref)
    logging.info(f'Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_ref}')


def create_aggregated_table(project_id, bq_client, variables):
    table_id = f'{project_id}.{variables["dataset_id"]}.{variables["query_table_id"]}'
    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    sql = """
    SELECT Creator AS creator, 
      SUM(revenue) AS total_revenue, 
      COUNT(DISTINCT productName) AS number_different_products, 
      COUNT(productName) AS total_products_sold,
      SUM(profit) AS total_profits
    FROM `spheric-subject-361720.sales.products` 
    GROUP BY 1
    """

    query_job = bq_client.query(sql, job_config=job_config)
    query_job.result()  # Wait for the job to complete.

    logging.info(f'Query results loaded to the table {table_id}')
