import json
import boto3
import requests
import os
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    api_key = os.environ.get('AQIAPIKEY')
    resourceID= os.environ.get('AQIResourceID')
    url  = f"https://api.data.gov.in/resource/{resourceID}?api-key={api_key}&format=json"    

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"AQI API Error: {response.text}")
    
    data = response.json()

    bucket_name = os.environ['BUCKET_NAME']
    date_folder = datetime.now().strftime('%m-%Y')
    file_name = f"aqi_data_{datetime.now().strftime('%Y%m%d')}.json"
    s3_key = f"AQI_Data/{date_folder}/{file_name}"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Saved AQI data to s3://{bucket_name}/{s3_key}")

    return {
        'statusCode': 200,
        'body': json.dumps('Data ingested successfully!')
    }