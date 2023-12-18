# -*- coding: utf-8 -*-
"""
Created on Tue Nov 14 14:40:33 2023

@author: shahs10
"""

import boto3
import json
import time
from os import environ
from datetime import datetime


dynamodb = boto3.client('dynamodb')
sqs = boto3.client(service_name='sqs')

table_name = environ.get('TABLE_NAME', "inprocess_check_table") 
status_table_name=environ.get('STATUS_TABLE_NAME', "queue_status_table") 
QUEUE_URL = environ.get('SQS_QUEUE_URL', "https://sqs.us-east-1.amazonaws.com/576737476547/testingqueue")
DEFAULT_EXECUTION_TIME = int(environ.get('DEFAULT_EXECUTION_TIME', 25))


def get_messages_count(queue_url):
    response = sqs.get_queue_attributes(
    QueueUrl=queue_url,
    AttributeNames=[
        'All'
        ]
    )
    return int(response['Attributes'].get('ApproximateNumberOfMessages',0)) + int(response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))

def calculate_remaining_time(messages_in_queue):
    return (messages_in_queue * DEFAULT_EXECUTION_TIME) + DEFAULT_EXECUTION_TIME

def update_status_table():
    primary_key = {
        'id': {'S': 'queue'},
    }
    updated_at = datetime.now().isoformat()
    updated_status='not_empty'
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
def lambda_handler(event, context):
    """
        Takes an API request, Extract Prompt, Stores in SQS queue, generate UUID
        Returns UUID
    """
    
    try:
        # body = json.loads(event['body'])
        # prompt = body.get('prompt')
        prompt="black horse"
        uuid = int(time.time()*1000)
    except Exception as excp:
        return {
            "statusCode": 400,
            "body" : json.dumps({"error": str(excp)})
            }
    
    if not prompt or len(prompt) > 500 :
        return {
            "statusCode": 400,
            "body" : json.dumps({"error": "Prompt must be provided and be less than 500 characters"})
            }
    
    try:
        response = sqs.send_message(
            QueueUrl = QUEUE_URL,
            MessageBody=json.dumps({'message': prompt})
            )
        messageId = response['MessageId']
        update_status_table()
        item = {
            'UUID': {'S': messageId},
            'prompt': {'S': prompt},
            'prompt_status': {"S": "queued"}
        }
        response2 = dynamodb.put_item(
            TableName=table_name,
            Item=item
        )
        print(response2)
        messages_in_queue = get_messages_count(QUEUE_URL)
        time_remaining = calculate_remaining_time(messages_in_queue)
        
    except Exception as excp:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(excp)})
            }
    return {
        "statusCode": 200,
        "body": json.dumps({'UUID': response['MessageId'],
                            "EXPECTED_TIME_REMAINING": time_remaining })
        }