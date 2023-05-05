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
import logging

from flask import Flask, request

#from cloudevents.http import from_http

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import dataform_v1beta1


project_id = "dataops-terraform"

app = Flask(__name__)

version = "1.0"

df_client = dataform_v1beta1.DataformClient()
 
 
def execute_workflow(repo_uri: str, compilation_result: str):
    """Run workflow based on the compilation"""
    request = dataform_v1beta1.CreateWorkflowInvocationRequest(
        parent=repo_uri,
        workflow_invocation=dataform_v1beta1.types.WorkflowInvocation(
            compilation_result=compilation_result
        )
    )
 
    response = df_client.create_workflow_invocation(request=request)
    name = response.name
    logging.info(f'created workflow invocation {name}')
    return name
 
 
def compile_workflow(repo_uri: str, gcp_project, bq_dataset: str, branch: str):
    """Compiles the code"""
    request = dataform_v1beta1.CreateCompilationResultRequest(
        parent=repo_uri,
        compilation_result=dataform_v1beta1.types.CompilationResult(
            git_commitish=branch,
            code_compilation_config=dataform_v1beta1.types.CompilationResult.CodeCompilationConfig(
                default_database=gcp_project,
                default_schema=bq_dataset,
            )
        )
    )
    response = df_client.create_compilation_result(request=request)
    name = response.name
    logging.info(f'compiled workflow {name}')
    return name
 
 
def get_workflow_state(workflow_invocation_id: str):
    """Checks the status of a workflow invocation"""
    while True:
        request = dataform_v1beta1.GetWorkflowInvocationRequest(
            name=workflow_invocation_id
        )
        response = df_client.get_workflow_invocation(request)
        state = response.state.name
        logging.info(f'workflow state: {state}')
        if state == 'RUNNING':
            time.sleep(10)
        elif state in ('FAILED', 'CANCELING', 'CANCELLED'):
            raise Exception(f'Error while running workflow {workflow_invocation_id}')
        elif state == 'SUCCEEDED':
            return
 
 
def run_workflow(gcp_project: str, location: str, repo_name: str, bq_dataset: str, branch: str):
    """Runs complete workflow, i.e. compile and invoke"""
 
    repo_uri = f'projects/{gcp_project}/locations/{location}/repositories/{repo_name}'
    compilation_result = compile_workflow(repo_uri, gcp_project, bq_dataset, branch)
    workflow_invocation_name = execute_workflow(repo_uri, compilation_result)
    get_workflow_state(workflow_invocation_name)


# Construct a BigQuery client object.
client = bigquery.Client()
    
# TODO(developer): Set table_id to the ID of the table to create.
table_id="dataops-terraform.dwh.wikipedia_pageviews_2021"

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
                TRUNCATE TABLE `dataops-terraform.dwh.wikipedia_pageviews_2021`
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
        
            #call dataform
            print("Calling Dataform Workdflow")
            location = 'us-central1'
            repo_name = 'dataform_gcs_to_bq_repository'
            bq_dataset = 'dwh'
            branch = 'bq-branch'
            #run_workflow(project_id, location, repo_name, bq_dataset, branch)

        else:
            msg = 'not a create object message'
            print(f'error: {msg}')
            return f'Bad Request: {msg}', 400 
    else:
            return f'Not the right message: {msg}', 400
    

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
