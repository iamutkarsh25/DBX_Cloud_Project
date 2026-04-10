import json
import boto3
import requests
import os
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    api_key = os.environ.get('NASA_API_KEY', 'DEMO_KEY')
    url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"NASA API error: {response.text}")

    data = response.json()

    bucket_name = os.environ['BUCKET_NAME']
    file_name = f"nasa_data_{datetime.now().strftime('%Y%m%d')}.json"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Saved NASA data to s3://{bucket_name}/{file_name}")

    return {
        'statusCode': 200,
        'body': json.dumps('Data ingested successfully!')
    }
