import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):

    #Lambda function that handles Baserow updates and reflects them in a DynamoDB table.
    
    table_name = 'student'
    table = dynamodb.Table(table_name)

    # Check if the DynamoDB table exists, and create it if it doesn't
    try:
        table.table_status
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Create the table if it doesn't exist
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'id',
                        'AttributeType': 'N'
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            table.wait_until_exists()
        else:
            raise e

    # Handle the different Baserow events
    event_type = event['event_type']
    if event_type == 'rows.updated':
        handle_baserow_update(table, event['items'], event['old_items'])
    elif event_type == 'rows.created':
        handle_baserow_create(table, event['items'])
    elif event_type == 'rows.deleted':
        handle_baserow_delete(table, event['row_ids'])  # Added this line
    else:
        return {
            'statusCode': 400,
            'body': 'Unsupported event type'
        }

    return {
        'statusCode': 200,
        'body': 'Baserow event processed successfully'
    }

def handle_baserow_update(table, items, old_items):
    """
    Handles updates from Baserow and updates the DynamoDB table.
    """
    for i, item in enumerate(items):
        old_item = old_items[i]
        item['updated_at'] = str(datetime.now())
        table.put_item(Item=item)

def handle_baserow_create(table, items):
    """
    Handles new items created in Baserow and adds them to the DynamoDB table.
    """
    for item in items:
        item['created_at'] = str(datetime.now())
        table.put_item(Item=item)

def handle_baserow_delete(table, row_ids):
    """
    Handles items deleted in Baserow and deletes them from the DynamoDB table.
    """
    for row_id in row_ids:
        table.delete_item(Key={'id': row_id})
