import logging
import json
import os
import sys
import re
# sys.path.append("/home/ec2-user/SageMaker/llm_bedrock_v0/")
from boto_client import Clientmodules
import time
import pandas as pd
import io

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class AthenaQueryExecute:
    def __init__(self):

        # Ensure the bucket name is all lowercase, as required by S3.
        # And since we're talking about S3, the quickest/easiest way to snag the bucket
        # name is to hop into the S3 console and copy/paste it.
        self.glue_databucket_name = 'athena-query-results-text-to-sql'
        self.athena_client = Clientmodules.createAthenaClient()
        self.s3_client = Clientmodules.createS3Client()

    def execute_query(self, query_string):
        result_folder = 'athena_output'
        result_config = {"OutputLocation": f"s3://{self.glue_databucket_name}/{result_folder}"}
        query_execution_context = {
            "Catalog": "AwsDataCatalog",
        }
        print(f"Executing: {query_string}")
        query_execution = self.athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration=result_config,
            QueryExecutionContext=query_execution_context,
        )
        execution_id = query_execution["QueryExecutionId"]
        time.sleep(120)

        file_name = f"{result_folder}/{execution_id}.csv"
        logger.info(f'Checking for file: {file_name}')
        local_file_name = f"./tmp/{file_name}"

        print(f"Calling download file with params {local_file_name}, {result_config}")
        obj = self.s3_client.get_object(Bucket=self.glue_databucket_name, Key=file_name)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
        return df

    def syntax_checker(self, query_string):
        print("Inside syntax_checker", query_string)
        query_result_folder = 'athena_query_output/'
        query_config = {"OutputLocation": f"s3://{self.glue_databucket_name}/{query_result_folder}"}
        query_execution_context = {
            "Catalog": "AwsDataCatalog",
        }
        query_string = "Explain  " + query_string
        print(f"Executing: {query_string}")
        try:
            print(" I am checking the syntax here")
            query_execution = self.athena_client.start_query_execution(
                QueryString=query_string,
                ResultConfiguration=query_config,
                QueryExecutionContext=query_execution_context,
            )
            execution_id = query_execution["QueryExecutionId"]
            print(f"execution_id: {execution_id}")
            time.sleep(3)
            results = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            status = results['QueryExecution']['Status']
            print("Status:", status)
            if status['State'] == 'SUCCEEDED':
                return "Passed"
            else:
                print(results['QueryExecution']['Status']['StateChangeReason'])
                errmsg = results['QueryExecution']['Status']['StateChangeReason']
                return errmsg
        except Exception as e:
            print("Error in exception")
            msg = str(e)
            print(msg)
