import csv
import tempfile
import re
import chardet
from google.cloud import bigquery, storage

def sanitize_field_name(name):
    """
    Sanitizes a field name to conform to BigQuery requirements.
    
    :param name: Original field name from the CSV header.
    :return: Sanitized field name.
    """
    sanitized_name = re.sub(r'\W', '_', name)
    return sanitized_name[:128]

def create_bq_table_from_header(dataset_id: str, table_id: str, header: list):
    """
    Creates a BigQuery table based on the CSV header.
    
    :param dataset_id: BigQuery dataset ID.
    :param table_id: BigQuery table ID.
    :param header: List of headers from the CSV file.
    """
    client = bigquery.Client()

    schema = [bigquery.SchemaField(sanitize_field_name(name), "STRING") for name in header]

    table_ref = client.dataset(dataset_id).table(table_id)

    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)

    print(f"Table {table_id} created in dataset {dataset_id}.")

def load_csv_to_bq(source_uri: str, dataset_id: str, table_id: str):
    """
    Loads a CSV file from Google Cloud Storage into a BigQuery table.
    
    :param source_uri: Source URI of the CSV file in Google Cloud Storage.
    :param dataset_id: BigQuery dataset ID.
    :param table_id: BigQuery table ID.
    """
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    table_ref = client.dataset(dataset_id).table(table_id)

    load_job = client.load_table_from_uri(
        source_uri,
        table_ref,
        job_config=job_config
    )

    load_job.result()
    print(f"CSV file from {source_uri} loaded into table {table_id}.")

def process_csv_file(event, context):
    """
    Cloud Function to process new CSV files in a Google Cloud Storage bucket
    and load them into BigQuery.
    
    :param event: Event triggered by Google Cloud Storage.
    :param context: Event context.
    """
    bucket_name = "csv-processing-bucket"
    dataset_id = "csv_imports"
    file_name = event['name']
    table_id = file_name.replace(".csv", "")

    source_uri = f"gs://{bucket_name}/{file_name}"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    with tempfile.NamedTemporaryFile() as temp_file:
        blob.download_to_filename(temp_file.name)     
        
        with open(temp_file.name, 'rb') as f:
            result = chardet.detect(f.read())
            encoding = result['encoding']
        
        with open(temp_file.name, 'r', encoding=encoding) as csv_file:
            reader = csv.reader(csv_file)
            header = next(reader)

    create_bq_table_from_header(dataset_id, table_id, header)
    load_csv_to_bq(source_uri, dataset_id, table_id)
