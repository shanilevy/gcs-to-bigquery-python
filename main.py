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

import os
import base64
import requests
import time
import json

from flask import Flask, request, make_response

#from cloudevents.http import from_http

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import workflows_v1
from google.cloud.workflows import executions_v1
from google.cloud.workflows.executions_v1.types import executions


project_id = "dataops-terraform-example"
#project_id = os.getenv("GCP_PROJECT")
location = 'us-central1'
workflow = 'dataform-workflow' 

if not project_id:
    raise Exception('GOOGLE_CLOUD_PROJECT env var is required.')

# Set up API clients.
execution_client = executions_v1.ExecutionsClient()
workflows_client = workflows_v1.WorkflowsClient()

# Construct the fully qualified location path.
parent = workflows_client.workflow_path(project_id, location, workflow)

app = Flask(__name__)

version = "1.0"


# Construct a BigQuery client object.
client = bigquery.Client()
    
# TODO(developer): Set table_id to the ID of the table to create.
table_id="dataops-terraform-example.dwh.wikipedia_pageviews_2021"

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
    print("data message:",data['message'])
    
    if 'attributes' in data['message']: 
        url = "gs://"+data['message']['attributes']['bucketId']+"/"+data['message']['attributes']['objectId']
        
        print("New file to add to BQ:",url)
        print("event type:",data['message']['attributes']['eventType'])

        if data['message']['attributes']['eventType'] == "OBJECT_FINALIZE":
            #before loading clear the staging table
            query_job = client.query(
                """
                TRUNCATE TABLE `dataops-terraform-example.dwh.wikipedia_pageviews_2021`
                """
            )

            #results = query_job.result()  # Waits for job to complete.
            
            #load new data from csv file into BQ table
            load_job = client.load_table_from_uri(
            url, table_id, job_config=job_config
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.

            destination_table = client.get_table(table_id)  # Make an API request.
            print("Loaded {} rows.".format(destination_table.num_rows))

            #after loading the csv file into BQ - calling dataform API to perform the transformation
            # base_url='https://api.dataform.co/v1/project/6283092602912768/run'
            # secret = os.environ.get("df_token")
            # headers={'Authorization': 'Bearer ' + secret}
            # run_create_request={"environmentName": "", "scheduleName": ""}

            # response = requests.post(base_url, data=json.dumps(run_create_request), headers=headers)

            # run_url = base_url + '/' + response.json()['id']

            # response = requests.get(run_url, headers=headers)

            # while response.json()['status'] == 'RUNNING':
            #     time.sleep(10)
            #     response = requests.get(run_url, headers=headers)
            #     print(response.json())
            
            # return (resp, 204)

            # Execute the workflow.
            response = execution_client.create_execution(request={"parent": parent})
            print(f"Created execution: {response.name}")

            # Wait for execution to finish, then print results.
            execution_finished = False
            backoff_delay = 1  # Start wait with delay of 1 second
            print('Poll every second for result...')
            while (not execution_finished):
                execution = execution_client.get_execution(
                    request={"name": response.name})
                execution_finished = execution.state != executions.Execution.State.ACTIVE

                # If we haven't seen the result yet, wait a second.
                if not execution_finished:
                    print('- Waiting for results...')
                    time.sleep(backoff_delay)
                    # Double the delay to provide exponential backoff.
                    backoff_delay *= 2
                else:
                    print(f'Execution finished with state: {execution.state.name}')
                    print(f'Execution results: {execution.result}')
                    #return execution
                    #return jsonpickle.encode(execution)
                    #return make_response(execution) 
        else:
            msg = 'not a create object message'
            print(f'error: {msg}')
            return f'Bad Request: {msg}', 400 
    else:
            return f'Not the right message: {msg}', 400
    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
