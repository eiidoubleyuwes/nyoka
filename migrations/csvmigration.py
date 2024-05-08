import os
import boto3
import csv

# connect to DynamoDB local
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

#CSV file and DynamoDB table details
CSV_FILE = 'migrations/students.csv'
TABLE_NAME = 'students'

def migrate_data():
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
        #Error handling by Dynamodb if created table already exixts
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        table = dynamodb.Table(TABLE_NAME)#table object
        print(f"Table {TABLE_NAME} already exists")

    # Write data from CSV file to DynamoDB
    with open(CSV_FILE, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            item = {
                'StudentID': row['StudentID'],
                'Enrolment_status': row['Enrolment_status'],
                'Name': row['Name'],
                'Email': row['Email'],
                'Phone': row['Phone'],
                'Start_date': row['Start_date'],
                'Program': row['Program'],
                'Finance_plan': row['Finance_plan']
            }
            table.put_item(Item=item)
    print("Data migration complete.")

if __name__ == "__main__":
    migrate_data()
