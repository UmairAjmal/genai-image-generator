import os
import boto3
from datetime import datetime

SERVICE_TABLE_NAME = os.environ.get('SERVICE_TABLE_NAME', "genai-image-gen-service-table") 

dynamodb = boto3.client('dynamodb')
sagemaker_client=boto3.client('sagemaker')

def is_diff_10_min(last_updated):
    # Define reference time
    reference_time = datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%f")
    
    # Get current time
    current_time = datetime.now()
    
    # Calculate the time difference in seconds
    time_diff = (current_time - reference_time).total_seconds()

    # Check if the difference is 10 minutes (600 seconds)
    return True if time_diff >= 600 else False

def get_service_details(service_type):
    primary_key = {
        "service_type": {"S": service_type}
    }
    
    # Get the item
    response = dynamodb.get_item(
        TableName=SERVICE_TABLE_NAME,
        Key=primary_key
    )
    
    if "Item" in response:
        # Get the status value
        return response["Item"]
    else:
        return None

def update_service_details(service_type):
    primary_key = {
        'service_type': {'S': service_type},
    }

    # Set the update expression
    update_expression = 'REMOVE service_name, updated_at'
    
    # Update the item
    response = dynamodb.update_item(
        TableName=SERVICE_TABLE_NAME,
        Key=primary_key,
        UpdateExpression=update_expression
    )


def get_inference_endpoint_status(endpoint_name):
    response = sagemaker_client.list_endpoints(
        NameContains=endpoint_name,
    )

    endpoints = response['Endpoints']
    print({"List of endpoints": endpoints})
    status = None
    if len(endpoints) > 0:
        status = endpoints[0]['EndpointStatus']

    return status

def lambda_handler(event, context):
    
    endpoint_details = get_service_details('inference_endpoint')
    queue_details = get_service_details('queue')
    
    print(endpoint_details)
    print(queue_details)
    queue_status=queue_details["service_status"]['S']

    # Process the item data if found
    time_updated=queue_details["updated_at"]['S']
    delete_endpoint = is_diff_10_min(time_updated)
    print({"Delete":delete_endpoint})
    endpoint_name =  endpoint_details['service_name']['S'] if 'service_name' in endpoint_details else None
    if  delete_endpoint == True and queue_status == 'empty' and endpoint_name != None:
        status = get_inference_endpoint_status(endpoint_name)
        if(status == 'InService'):
            response = sagemaker_client.delete_endpoint(
                EndpointName=endpoint_details['service_name']['S']
            )
            update_service_details('inference_endpoint')
            print("successfully deleted endpoint after 10 minutes")
        else:
            print("No endpoint exist for deletion")
    else:
        print('Condition for deletion not met')

