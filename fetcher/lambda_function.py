import boto3
import json
from os import environ

S3_BUCKET = environ.get('S3_BUCKET', 'stablediffusion-imagegeneration')
REQUEST_TABLE_NAME = environ.get('REQUST_TABLE_NAME', "genai-image-gen-request-table") 
SERVICE_TABLE_NAME = environ.get('SERVICE_TABLE_NAME', "genai-image-gen-service-table") 
QUEUE_URL = environ.get('SQS_QUEUE_URL', "https://sqs.us-east-1.amazonaws.com/576737476547/genai-image-gen-queue.fifo")

s3 = boto3.client("s3")
sqs = boto3.client('sqs')
dynamodb = boto3.client('dynamodb')

def check_request_status(request_id):
    # Set the primary key values
    primary_key = {
        "uuid": {"S": request_id}
    }
    
    # Get the item
    response = dynamodb.get_item(
        TableName=REQUEST_TABLE_NAME,
        Key=primary_key
    )
    print({'Request ID': request_id, 'Request response': response})

    if "Item" in response:
        # Get the status value
        status = response["Item"]["prompt_status"]["S"]
        return status
    else:
        return "Not found"

def presigned_url(bucket,key):
    print("Presigned URL")
    url = s3.generate_presigned_url(
                ClientMethod="get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=1000,
            )
    return url


def lambda_handler(event, context):
    try:

        body = json.loads(event.get('body', {}))
        request_id = body.get('request_id')
        
        status=check_request_status(request_id)
        if status == "queued":
            # messages_in_queue = get_messages_count(QUEUE_URL)
            # time_remaining = calculate_remaining_time(messages_in_queue)
            return {
                "statusCode": 202,
                "body": json.dumps({'request_id': request_id, 'status': status})
            }
        elif status == "inprogress":
            return {
                "statusCode": 202,
                "body": json.dumps({'request_id': request_id, 'status': status})
            }
        elif status == "completed":
            key = request_id + ".jpg"
            image_url = presigned_url(S3_BUCKET,key)
            print(image_url)
            return {
                "statusCode": 200,
                "body": json.dumps({'request_id': request_id, 'status': status, 'url': image_url })
            }
        else:
            return {
                "statusCode": 500,
                "body": json.dumps({'request_id': request_id, 'status': status })
            }
    except Exception as e:
        print(e)
        return {
                "statusCode": 500,
                "body": json.dumps({'status': f"{e}" })
        }