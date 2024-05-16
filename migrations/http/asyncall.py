import os
import json
import boto3
from botocore.vendored import requests

def lambda_handler(event, context):
    # Fetch data from the database
    data = fetch_data_from_database()

    # Write the data to DynamoDB
    write_to_dynamodb(data)

    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }

def fetch_data_from_database():
    url = "https://base.zinduaschool.com/api/database/rows/table/601/?user_field_names=true"
    headers = {
        "Authorization": f"Token {os.environ['DATABASE_TOKEN']}"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Extract the relevant fields
    results = []
    for item in data['results']:
        results.append({
            'StudentID': item['StudentID'],
            'Sname': item['Sname'],
            'email': item['email'],
            'phone': item['phone']
        })

    return results

def write_to_dynamodb(data):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['DYNAMODB_TABLE_NAME'])

    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)
