from google.cloud import bigquery
from google.oauth2 import service_account

project_id = "mhanline-sand07"
key_path = "credentials.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# Construct a BigQuery client object.
#client = bigquery.Client()
    
# TODO(developer): Set table_id to the ID of the table to create.
table_id = "mhanline-sand07.dwh.wikipedia_pageviews_2021"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("datehour", "TIMESTAMP"),
        bigquery.SchemaField("wiki", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("views", "INTEGER"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
uri = "gs://my-dataops-bucket-123/wikipedia_pageviews_2021-000000000000.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Waits for the job to complete.

destination_table = client.get_table(table_id)  # Make an API request.
print("Loaded {} rows.".format(destination_table.num_rows))
# [END bigquery_load_table_gcs_csv]
