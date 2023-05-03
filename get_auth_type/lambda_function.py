import json
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
#from opensearchpy import OpenSearch, RequestsHttpConnection
import requests
from decimal import Decimal
import datetime
import collections
from urllib.parse import quote
import sys
if sys.version_info.major == 3 and sys.version_info.minor >= 10:
    from collections.abc import MutableMapping
else:
    from collections import MutableMapping
import time

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user-profile')


def lambda_handler(event, context):
    
    # testing display on front-end
    print('event: ', json.dumps(event))
    
    # use to delete entries from table
    #table.delete_item(Key={'email': 'sarahtang07@gmail.com'})
    
    # test: print contents of table
    response = table.scan()
    items = response['Items']
    print('Table Size: ', len(items))
    print('Table Contents:')
    for item in items:
        print(item)
    
    email = event['email']
    info = table.get_item(Key={"email": email})
    if 'Item' not in info:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({"newUser": True})
        }
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({"newUser": False})
    }