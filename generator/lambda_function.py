# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 15:54:47 2023

@author: shahs10
"""

import boto3
import json
from os import environ
import time
import base64
from PIL import Image
from io import BytesIO
from datetime import datetime


REQUEST_TABLE_NAME = environ.get('REQUST_TABLE_NAME', "genai-image-gen-request-table") 
SERVICE_TABLE_NAME = environ.get('SERVICE_TABLE_NAME', "genai-image-gen-service-table") 
QUEUE_URL = environ.get('SQS_QUEUE_URL', "https://sqs.us-east-1.amazonaws.com/576737476547/genai-image-gen-queue.fifo")

s3 = boto3.client("s3")
sqs = boto3.client('sqs')
dynamodb = boto3.client('dynamodb')
comprehend = boto3.client('comprehend')
sagemaker_client = boto3.client('sagemaker')
sagemaker_runtime_client = boto3.client("sagemaker-runtime")

SEED = int(environ.get('SEED', 42))
STEPS = int(environ.get("STEPS", 40))
WIDTH = int(environ.get('WIDTH', 768)) 
WEIGHT = int(environ.get('WEIGHT', 1))
HEIGHT = int(environ.get('HEIGHT', 768))
SAMPLES = int(environ.get('SAMPLES', 1))
CFG_SCALE = int(environ.get('CFG_SCALE', 7))
SAMPLER = environ.get('SAMPLER', 'K_DPMPP_2M')
LANGUAGE_CODE = environ.get('LANGUAGE_CODE', 'en')
CONTENT_TYPE = environ.get('CONTENT_TYPE', "application/json")
SQS_NEW_DELAY_TIME = int(environ.get('SQS_NEW_DELAY_TIME', 120))
S3_BUCKET = environ.get('S3_BUCKET', 'stablediffusion-imagegeneration')
SQS_DEFAULT_DELAY_TIME = int(environ.get('SQS_DEFAULT_DELAY_TIME', 0))
ENDPOINT_CONFIG_NAME = environ.get('ENDPOINT_CONFIG_NAME', 'genai-image-gen-endpoint-config')

def update_service_status(service_type, status = None, service_name = None):
    primary_key = {
        'service_type': {'S': service_type},
    }
    updated_at = datetime.now().isoformat()

    if service_type == 'inference_endpoint':
        # Set the update expression
        update_expression = 'SET service_name  = :service_name, updated_at = :updated_at'
        expression_attribute_values = {
            ':service_name': {'S': service_name},
            ':updated_at': {'S': updated_at}
        }

    if service_type == 'queue':
        # Set the update expression
        update_expression = 'SET service_status = :new_status, updated_at = :updated_at'
        expression_attribute_values = {
            ':new_status': {'S': status},
            ':updated_at': {"S": updated_at}
        }
    
    # Update the item
    response = dynamodb.update_item(
        TableName=SERVICE_TABLE_NAME,
        Key=primary_key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    
def update_request_record(request_id, status, url = None):
    primary_key = {
        'uuid': {'S': request_id},
    }

    # Set the update expression
    update_expression = 'SET prompt_status = :new_status'
    expression_attribute_values = {
        ':new_status': {'S': status}
    }

    if url is not None:
        update_expression += ', output_image_url = :url'
        expression_attribute_values[':url'] = {'S':url}
    
    # Update the item
    response = dynamodb.update_item(
        TableName=REQUEST_TABLE_NAME,
        Key=primary_key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    
    # Print the response
    print(response)  
    
def is_prompt_positive(prompt):
    print("Is prompt positive")
    response = comprehend.detect_sentiment(Text=prompt, LanguageCode=LANGUAGE_CODE)
    
    sentiment = response['Sentiment']
    score = response['SentimentScore']
    
    # Print the sentiment and score
    print('Sentiment:', sentiment)
    print('Score:', score)

    if sentiment.upper() in ('POSITIVE', 'NEUTRAL'):
        print("Prompt is Positive")
        return True
    else:
        print("Prompt is Negative")
        return False
    

def get_prompt(event):
    print("Get prompt")    
    record = event.get('Records')[0]
    request_id, prompt = record['messageId'], json.loads(record['body']).get('message')
    return request_id, prompt

def generate_image(request_id, prompt, endpoint_name):
    print("Generate Image")
    payload = {
        "cfg_scale": CFG_SCALE,
        "height": HEIGHT,
        "width": WIDTH,
        "steps": STEPS,
        "seed": SEED,
        "sampler": SAMPLER,
        "text_prompts": [
            {
                "text": prompt,
                "weight": WEIGHT
            }
        ],
        "samples": SAMPLES  # Set samples to 1 for a single image
    }
    
    print("Prompt ", prompt)

    try:
        print("Invoking endpoint")
        status="inprogress"
        update_request_record(request_id,status)
        response = sagemaker_runtime_client.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType=CONTENT_TYPE,
            Body=json.dumps(payload)
        )
        print(" RESPONSE OF generate image is ", response)
        return 200, response
    except Exception as excp:
        status="failed"
        update_request_record(request_id, status)
        print("Exception ", excp)
        return 500, json.dumps({"error": str(excp)})

def store_image_in_s3(request_id, response):
    try:
        result = json.loads(response['Body'].read())
        base64_data= result['artifacts'][0]['base64']
        image_data = base64.b64decode(base64_data)
        image = Image.open(BytesIO(image_data))
        s3_key = request_id + ".jpg"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=image_data)
        print("S3 KEY ", s3_key)
        # url=presigned_url(S3_BUCKET,s3_key)
        return 200, s3_key
    except Exception as excp:
        status="failed"
        update_request_record(request_id, status)
        print("Exception ", excp)
        return 500,  json.dumps({"error": str(excp)})

def get_messages_count(queue_url):
    response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=[
        'All'
        ]
    )
    return int(response['Attributes'].get('ApproximateNumberOfMessages',0)) + int(response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))

def create_inference_endpoint(endpoint_name):
    sagemaker_client.create_endpoint(
        EndpointName= endpoint_name,
        EndpointConfigName= ENDPOINT_CONFIG_NAME
    )
    return endpoint_name

def get_inference_endpoint():
    dynamodb = boto3.resource('dynamodb')
    dynamo_table_client = dynamodb.Table(SERVICE_TABLE_NAME)
    response = dynamo_table_client.get_item(
        Key={
            'service_type': 'inference_endpoint',
        }
    )

    # Check if item exists and return the record
    if 'Item' in response:
        item = response['Item']
        return item.get('service_name', None)
    else:
        return None, None

def set_sqs_delay(delay_time):
    response = sqs.set_queue_attributes(
        QueueUrl=QUEUE_URL,
        Attributes={
            'DelaySeconds': str(delay_time)
        }
    )

def get_inference_endpoint_status(endpoint_name):
    response = sagemaker_client.describe_endpoint(
        EndpointName=endpoint_name
    )

    return response.get('EndpointStatus', None)

def lambda_handler(event, context):
    print("EVENT ", event)
    record = event.get('Records')[0]
    try:
        # Get Prompt
        request_id, prompt = get_prompt(event)

        print("Prompt :: ", prompt)
        print("Request ID  :: ",  request_id )

        # Send Stable Diffusion Request for Image Generation
        if is_prompt_positive(prompt):
            # Check for existing inference endpoint status
            endpoint_name = get_inference_endpoint()

            if endpoint_name is None:
            # Create new endpoint if not present
                current_date = datetime.now().strftime("%Y-%m-%d-%H-%M")
                endpoint_name = f'genai-image-gen-{current_date}'
                update_service_status('inference_endpoint', None, endpoint_name)
                create_inference_endpoint(endpoint_name)
                set_sqs_delay(SQS_NEW_DELAY_TIME)
                raise Exception('Endpoint not present, creation in progress')
            
            # Check for endpoint status, if not inservice raise error
            status = get_inference_endpoint_status(endpoint_name)
            if(status != 'InService'):
                raise Exception('Endpoint not in service')
            
            set_sqs_delay(SQS_DEFAULT_DELAY_TIME)
            statusCode, response = generate_image(request_id, prompt, endpoint_name)
            
            print("Response of generate image ", response)
            print("status code of gen image : ",  statusCode )
            
            
            if statusCode == 500:
                return {
                    "statusCode": 500,
                    "body" : response
                    }
            else:               # store in S3
                statusCode, s3_key = store_image_in_s3(request_id, response)
                
                print("Response of store image ", s3_key)
                print("status code of store image : ",  statusCode )
                
                if statusCode == 500:
                    
                    return {
                        "statusCode": 500,
                        "body" : response
                        }
                else: # Remove from SQS
                    status="completed"
                    # delete_from_sqs(QUEUE_URL, ReceiptHandle)
                    messages_in_queue = get_messages_count(QUEUE_URL)
                    if messages_in_queue == 0:
                        status = 'empty'
                        update_service_status('queue', status)
                    update_request_record(request_id,status, url)
                    return {
                        'statusCode': 200,
                        "body" : json.dumps({"Processed UUID": request_id})
                    }
        
    except Exception as excp:
        print("Exception ", excp)
        raise excp
