import json
import uuid
import boto3
from os import environ
from datetime import datetime


dynamodb = boto3.client('dynamodb')
sqs = boto3.client(service_name='sqs')

DEFAULT_EXECUTION_TIME = int(environ.get('DEFAULT_EXECUTION_TIME', 25))
REQUEST_TABLE_NAME = environ.get('REQUST_TABLE_NAME', "genai-image-gen-request-table") 
SERVICE_TABLE_NAME = environ.get('SERVICE_TABLE_NAME', "genai-image-gen-service-table") 
QUEUE_URL = environ.get('SQS_QUEUE_URL', "https://sqs.us-east-1.amazonaws.com/576737476547/genai-image-gen-queue.fifo")


def get_messages_count(queue_url):
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['All']
    )
    return int(response['Attributes'].get('ApproximateNumberOfMessages',0)) + int(response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))

def calculate_remaining_time(messages_in_queue):
    return (messages_in_queue * DEFAULT_EXECUTION_TIME) + DEFAULT_EXECUTION_TIME

def update_queue_service_status(update_time):
    primary_key = {
        'service_type': {'S': 'queue'},
    }
    updated_status='not_empty'
    # Set the update expression
    update_expression = 'SET service_status = :new_status, updated_at = :updated_at'
    expression_attribute_values = {
        ':new_status': {'S': updated_status},
        ':updated_at': {"S": update_time}
    }
    
    # Update the item
    response = dynamodb.update_item(
        TableName=SERVICE_TABLE_NAME,
        Key=primary_key,
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values
    )

def record_request(request_id, prompt, created_at):
    item = {
        'uuid': {'S': request_id},
        'prompt': {'S': prompt},
        'prompt_status': {"S": "queued"},
        'created_at': {"S": created_at} 
    }
    print({'put item': item})
    response = dynamodb.put_item(
            TableName=REQUEST_TABLE_NAME,
            Item=item
        )
    print(response)

def save_message_in_sqs(message, request_id):
    response = sqs.send_message(
        QueueUrl = QUEUE_URL,
        MessageBody=json.dumps({'message': message}),
        MessageGroupId='gen-request',
        MessageDeduplicationId=request_id
    )
    return response['MessageId']
def lambda_handler(event, context):
    """
        Takes an API request, Extract Prompt, Stores in SQS queue, generate UUID
        Returns UUID and the queue number
    """
    
    try:
        body = json.loads(event['body'])
        prompt = body.get('prompt')
        creation_time = datetime.now().isoformat()
        request_id = str(uuid.uuid4())
        if not prompt or len(prompt) > 500 :
            return {
                "statusCode": 400,
                "body" : json.dumps({"error": "Prompt must be provided and be less than 500 characters"})
                }
        
        message_id = save_message_in_sqs(prompt, request_id)
        update_queue_service_status(creation_time)
        record_request(message_id, prompt, creation_time)
        
        messages_in_queue = get_messages_count(QUEUE_URL)
        time_remaining = calculate_remaining_time(messages_in_queue)
        return {
            "statusCode": 200,
            "body": json.dumps({
                'UUID': message_id,
                'QUEUE_NUMBER':messages_in_queue,
                'EXPECTED_TIME_REMAINING': time_remaining
            })
        }
    except Exception as excp:
        print(excp)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(excp)})
        }