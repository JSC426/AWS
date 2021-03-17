import os
import io
import boto3
import json
import csv

# grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
runtime= boto3.client('runtime.sagemaker')

def lambda_handler(event, context):

    
    payload = json.loads(json.dumps(event))['data'].encode('utf-8')

    response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                       ContentType='text/csv',
                                       Body=payload)

    response_string = response['Body'].read()#.decode('utf-8')
    response_dict = json.loads(response_string)

    #indicies_string = response_dict[1]
    #sims_string = response_dict[0]
    embeddings = response_dict[0]
    
    #indicies = list(map(lambda x: int(x), indicies_string))
    #sims = list(map(lambda x: float(x), sims_string))
    #sims = [round(1 - s, 3) for s in sims]
    
    #embeddings = list(map(lambda x: float(x), embeddings_string))
        
    return [embeddings]

    # Get Embeddings Lambda
    # PlotEncoderLiteKNN