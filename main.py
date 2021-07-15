# Copyright 2020 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START cloudrun_helloworld_service]
# [START run_helloworld_service]
import os
import base64

from flask import Flask, request

#from cloudevents.http import from_http

from google.cloud import bigquery
from google.oauth2 import service_account
#from google.cloud import pubsub_v1


project_id = "dataops-319100"
subscirption = "gcs-new-file-sub"

#subscriber_client = pubsub_v1.SubscriberClient()

# existing subscription
#subscription = subscriber_client.subscription_path(
#    project_id, subscirption)

app = Flask(__name__)

#key_path = "credentials.json"

#credentials = service_account.Credentials.from_service_account_file(
#    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
#)

#client=bigquery.Client(credentials=credentials, project=credentials.project_id,)

# Construct a BigQuery client object.
client = bigquery.Client()
    
# TODO(developer): Set table_id to the ID of the table to create.
table_id="dataops-319100.dwh.wikipedia_pageviews_2021"

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
uri = "gs://tmer-dataops-bucket-123/wikipedia_pageviews_2021-000000000006.csv"

#load_job = client.load_table_from_uri(
#    uri, table_id, job_config=job_config
#)  # Make an API request.

#load_job.result()  # Waits for the job to complete.

#destination_table = client.get_table(table_id)  # Make an API request.
#print("Loaded {} rows.".format(destination_table.num_rows))

#@app.route("/")
#def hello_world():
#    name = os.environ.get("NAME", "World")
#    return "Hello {}!".format(name)
#@app.route("/", methods=["POST"])
#def home():
    # create a CloudEvent
#    print("This is POST command")
#    event = from_http(request.headers, request.get_data())

    # you can access cloudevent fields as seen below
#    print(
#        f"Found {event['id']} from {event['source']} with type "
#        f"{event['type']} and specversion {event['specversion']}"
#    )

 #   return "", 204
# [START eventarc_pubsub_handler]
@app.route('/', methods=['POST'])
def index():
    data = request.get_json()
    if not data:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(data, dict) or 'message' not in data:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = data['message']

    name = 'World'
    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        name = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()

    resp = f"Hello, {name}! ID: {request.headers.get('ce-id')}"
    #print(resp)
    url = "gs://"+data['message']['attributes']['bucketId']+"/"+data['message']['attributes']['objectId']
    print("New file to add to BQ:",url)
    print("event type:",data['message']['attributes']['eventType'])
    #desired_object_state = "OBJECT_FINALIZE"

    if data['message']['attributes']['eventType'] == "OBJECT_FINALIZE":
        load_job = client.load_table_from_uri(
        url, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        destination_table = client.get_table(table_id)  # Make an API request.
        print("Loaded {} rows.".format(destination_table.num_rows))

        #pubsub_message['data'].ack()

        return (resp, 200)
    else:
        msg = 'not the final message'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400 
# [END eventarc_pubsub_handler]

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    #app.run(port=8080)
# [END run_helloworld_service]
# [END cloudrun_helloworld_service]