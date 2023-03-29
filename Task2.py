import os
from google.cloud import bigquery
from google.auth import jwt
from google.oauth2 import service_account

def load_to_bigquery(event, context):
    # Extract the Pub/Sub message data from the event
    pubsub_message = event['data']
    name = pubsub_message['name']
    age = pubsub_message['age']
    email = pubsub_message['email']

    # Get the BigQuery configuration settings from environment variables
    project_id = os.environ['BQ_PROJECT']
    dataset_id = os.environ['BQ_DATASET']
    table_id = os.environ['BQ_TABLE']
    credentials_path = 'cred.json'

    # Create a BigQuery client using the credentials file
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Construct the BigQuery table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Create a BigQuery table reference object
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the table schema
    schema = [
    bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('email', 'STRING', mode='REQUIRED'),
    ]

    # Create the BigQuery table if it doesn't exist
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)

    # Insert the data into the BigQuery table
    rows_to_insert = [(name, age, email)]
    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Successfully inserted row {rows_to_insert} into {table_id}")
