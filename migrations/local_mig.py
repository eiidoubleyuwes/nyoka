import os
import boto3
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

# Set up Google Sheets API
creds = Credentials.from_info({
    'token': os.environ['GOOGLE_TOKEN'],
    'refresh_token': os.environ['GOOGLE_REFRESH_TOKEN'],
    'token_uri': 'https://oauth2.googleapis.com/token',
    'client_id': os.environ['GOOGLE_CLIENT_ID'],
    'client_secret': os.environ['GOOGLE_CLIENT_SECRET']
})
sheets_service = build('sheets', 'v4', credentials=creds)

# Set up DynamoDB local
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Google Sheet and DynamoDB table details
SHEET_ID = '1oTybIjzOG9EtPuFuS7jDXIhJd3L9OjLefjyRV1QMIxw'
SHEET_RANGE = 'Students!A1:H'
TABLE_NAME = 'students'

def migrate_data():
    # Fetch data from Google Sheet
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_RANGE
    ).execute()
    values = result.get('values', [])

    # Create the DynamoDB table if it doesn't exist
    try:
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'StudentID', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'StudentID', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print(f"Created table: {TABLE_NAME}")
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        table = dynamodb.Table(TABLE_NAME)
        print(f"Table {TABLE_NAME} already exists")

    # Write data from Google Sheet to DynamoDB
    for row in values[1:]:  # Skip the header row
        item = {
            'StudentID': row[0],
            'Enrolment_status': row[1],
            'Name': row[2],
            'Email': row[3],
            'Phone': row[4],
            'Start_date': row[5],
            'Program': row[6],
            'Finance_plan': row[7]
        }
        table.put_item(Item=item)
    print("Data migration complete.")

if __name__ == "__main__":
    migrate_data()
