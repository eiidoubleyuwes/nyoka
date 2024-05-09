import boto3
from datetime import datetime
import json

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table_name = 'student'
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    """
    Lambda function that performs CRUD operations on a DynamoDB table.
    """
    if 'httpMethod' in event:
        http_method = event['httpMethod']
        if http_method == 'GET':
            return handle_get_request(event)
        elif http_method == 'POST':
            return handle_post_request(event)
        elif http_method == 'PUT':
            return handle_put_request(event)
        elif http_method == 'DELETE':
            return handle_delete_request(event)
        else:
            return {
                'statusCode': 400,
                'body': 'Unsupported HTTP method'
            }
    elif 'event_type' in event:
        if event['event_type'] == 'rows.updated':
            return handle_baserow_update(event)
        elif event['event_type'] == 'rows.created':
            return handle_baserow_create(event)
        elif event['event_type'] == 'rows.deleted':
            return handle_baserow_delete(event)
        else:
            return {
                'statusCode': 400,
                'body': 'Unsupported event type'
            }
    else:
        return {
            'statusCode': 400,
            'body': 'Unsupported event type'
        }

def handle_get_request(event):
    """
    Handles GET requests to the DynamoDB table.
    """
    if 'id' in event['queryStringParameters']:
        # Get a specific item
        item_id = event['queryStringParameters']['id']
        response = table.get_item(Key={'id': item_id})
        return {
            'statusCode': 200,
            'body': json.dumps(response['Item'])
        }
    else:
        # Get all items
        response = table.scan()
        return {
            'statusCode': 200,
            'body': json.dumps(response['Items'])
        }

def handle_post_request(event):
    """
    Handles POST requests to the DynamoDB table.
    """
    item = json.loads(event['body'])
    item['created_at'] = str(datetime.now())
    response = table.put_item(Item=item)
    return {
        'statusCode': 200,
        'body': 'Item created'
    }

def handle_put_request(event):
    """
    Handles PUT requests to the DynamoDB table.
    """
    item = json.loads(event['body'])
    item['updated_at'] = str(datetime.now())
    response = table.put_item(Item=item)
    return {
        'statusCode': 200,
        'body': 'Item updated'
    }

def handle_delete_request(event):
    """
    Handles DELETE requests to the DynamoDB table.
    """
    item_id = event['queryStringParameters']['id']
    response = table.delete_item(Key={'id': item_id})
    return {
        'statusCode': 200,
        'body': 'Item deleted'
    }

def handle_baserow_update(event):
    """
    Handles updates from Baserow and updates the DynamoDB table.
    """
    for item in event['items']:
        table.put_item(Item=item)
    return {
        'statusCode': 200,
        'body': 'Items updated'
    }

def handle_baserow_create(event):
    """
    Handles new items created in Baserow and adds them to the DynamoDB table.
    """
    for item in event['items']:
        table.put_item(Item=item)
    return {
        'statusCode': 200,
        'body': 'Items created'
    }

def handle_baserow_delete(event):
    """
    Handles items deleted in Baserow and deletes them from the DynamoDB table.
    """
    for row_id in event['row_ids']:
        table.delete_item(Key={'id': str(row_id)})
    return {
        'statusCode': 200,
        'body': 'Items deleted'
    }
