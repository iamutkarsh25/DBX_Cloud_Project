import json
import boto3
import requests
import os
from datetime import datetime

s3-client = boto3.client('s3')

def lambda_handler(event, context):
    api_key = os.environ.get('MandiAPIKEY')
    resourceID= os.environ.get('resourceID')
    url  = f"https://api.data.gov.in/resource/{resourceId}}?api-key={api_key}&format=json"

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Mandi API error: {response.text}")
    
    data = response.json()

    bucket_name = os.environ['BUCKET_NAME']
    file_name = f"mandi_data_{datetime.now().strftime('%Y%m%d')}.json"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Saved Mandi data to s3://{bucket_name}/{file_name}")

    return {
        'statusCode': 200,
        'body': json.dumps('Data ingested successfully!')
    }