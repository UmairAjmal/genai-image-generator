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

sqs = boto3.client(service_name='sqs')
dynamodb = boto3.client('dynamodb')
QUEUE_URL = environ.get('SQLS_QUEUE_URL', "https://sqs.us-east-1.amazonaws.com/576737476547/generativeaiqueue")
table_name = environ.get('TABLE_NAME', "inprocess_check_table")
status_table_name=environ.get('STATUS_TABLE_NAME', "queue_status_table") 

s3 = boto3.client("s3")
comprehend = boto3.client('comprehend')
client = boto3.client("sagemaker-runtime")

LANGUAGE_CODE = environ.get('LANGUAGE_CODE', 'en')
ENDPOINT_NAME = environ.get('ENDPOINT_NAME', 'sdxl-1-0-jumpstart-2023-11-17-06-52-10-518')
CONTENT_TYPE = environ.get('CONTENT_TYPE', "application/json")
CFG_SCALE = int(environ.get('CFG_SCALE', 7))
HEIGHT = int(environ.get('HEIGHT', 768))
WIDTH = int(environ.get('WIDTH', 768)) 
STEPS = int(environ.get("STEPS", 40))
SEED = int(environ.get('SEED', 42))
SAMPLER = environ.get('SAMPLER', 'K_DPMPP_2M')
WEIGHT = int(environ.get('WEIGHT', 1))
SAMPLES = int(environ.get('SAMPLES', 1))
S3_BUCKET = environ.get('S3_BUCKET', 'stablediffusion-imagegeneration')

def update_status_table(status_table_name):
    primary_key = {
        'id': {'S': 'queue'},
    }
    updated_at = datetime.now().isoformat()
    updated_status='empty'
    # Set the update expression
    update_expression = 'SET queue_status = :new_status, updated_at = :updated_at'
    expression_attribute_values = {
        ':new_status': {'S': updated_status},
        ':updated_at': {"S": updated_at}
    }
    
    # Update the item
    response = dynamodb.update_item(
        TableName=status_table_name,
        Key=primary_key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )
    
def update_record_table(status,record):
    UUID = record['messageId']
    primary_key = {
        'UUID': {'S': UUID},
    }

    # Set the update expression
    update_expression = 'SET prompt_status = :new_status'
    expression_attribute_values = {
        ':new_status': {'S': status}
    }
    
    # Update the item
    response = dynamodb.update_item(
        TableName=table_name,
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
    
def presigned_url(bucket,key):
    print("Presigned URL")
    s3_client = boto3.client(
        "s3", config=boto3.session.Config(signature_version="s3v4")
    )

    url = s3_client.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=1000,
            )
    return url


def get_prompt(event):
    print("Get prompt")
    # response = sqs.receive_message(
    #     QueueUrl = queue_url,
    #     AttributeNames=['All'],
    #     MessageAttributeNames=['All'],
    #     MaxNumberOfMessages=1,
    #     VisibilityTimeout=205,
    #     WaitTimeSeconds=20 )
    # print("RESPONSE :: ", response)
    # messages = response.get('Messages')
    # UUID = messages[0].get('MessageId')
    # ReceiptHandle =  messages[0].get('ReceiptHandle')
    # prompt = json.loads(messages[0]['Body']).get('message')
    
    record = event.get('Records')[0]
    UUID, ReceiptHandle, prompt = record['messageId'], record['receiptHandle'], json.loads(record['body']).get('message')
    return UUID, ReceiptHandle, prompt

def generate_image(prompt,record):
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
        update_record_table(status,record)
        response = client.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType=CONTENT_TYPE,
        Body=json.dumps(payload))
        print(" RESPONSE OF generate image is ", response)
        return 200, response
    except Exception as excp:
        status="failed"
        update_record_table(status,record)
        print("Exception ", excp)
        return 500, json.dumps({"error": str(excp)})

def store_image_in_s3(UUID, response,record):
    try:
        result = json.loads(response['Body'].read())
        base64_data= result['artifacts'][0]['base64']
        image_data = base64.b64decode(base64_data)
        image = Image.open(BytesIO(image_data))
        s3_key = UUID + ".jpg"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=image_data)
        print("S3 KEY ", s3_key)
        
        # URL=presigned_url(S3_BUCKET,s3_key)
        return 200, s3_key
    except Exception as excp:
        status="failed to store in s3"
        update_record_table(status,record)
        print("Exception ", excp)
        return 500,  json.dumps({"error": str(excp)})

def delete_from_sqs(queue_url, receiptHandle):
    response = sqs.delete_message(QueueUrl = queue_url, ReceiptHandle = receiptHandle)
    print("delete response : ",response)    
    
def get_messages_count(queue_url):
    response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=[
        'All'
        ]
    )
    return int(response['Attributes'].get('ApproximateNumberOfMessages',0)) + int(response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
    
def lambda_handler(event, context):
    print("EVENT ", event)
    record = event.get('Records')[0]
    try:
        # Get Prompt
        UUID, ReceiptHandle, prompt = get_prompt(event)

        print("Response of get prompt  ", prompt)
        print("UUID of get prompt  : ",  UUID )
        print("Prompt :: ", prompt)

        # Send Stable Diffusion Request for Image Generation
        if is_prompt_positive(prompt):
            statusCode, response = generate_image(prompt,record)
            
            print("Response of generate image ", response)
            print("status code of gen image : ",  statusCode )
            
            
            if statusCode == 500:
                return {
                    "statusCode": 500,
                    "body" : response
                    }
            else:               # store in S3
                statusCode, URL = store_image_in_s3(UUID, response , record)
                
                print("Response of store image ", URL)
                print("status code of store image : ",  statusCode )
                
                if statusCode == 500:
                    
                    return {
                        "statusCode": 500,
                        "body" : response
                        }
                else: # Remove from SQS
                    status="completed"
                    delete_from_sqs(QUEUE_URL, ReceiptHandle)
                    messages_in_queue = get_messages_count(QUEUE_URL)
                    if messages_in_queue == 0:
                        update_status_table(status_table_name)
                    update_record_table(status,record)
                    return {
                        'statusCode': 200,
                        "body" : json.dumps({"Processed UUID": UUID})
                        }
        
    except Exception as excp:
        print("Exception ", excp)
        return {
            'statusCode': 500,
            'body': json.dumps(str(excp))
            }