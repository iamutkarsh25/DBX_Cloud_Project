def handler(event, context):
    print("Hi! This is the first code to deploy.")
    return {
        "statusCode": 200,
        "body": "OK"
    }
