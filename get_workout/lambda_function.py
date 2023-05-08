import base64
import json
import time
from datetime import *
import boto3
import re
import requests
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

dynamodb = boto3.resource('dynamodb')
w_table = dynamodb.Table('workouts')
user_table = dynamodb.Table('user-profile')

'''
inputs:
workoutid
email
'''
        
def lambda_handler(event, context):
    
    # testing display on front-end
    print('event: ', json.dumps(event))
    
    #EXTRACT WORKOUT ID#
    wid = event['workout_id']
    
    #EXTRACT EMAIL ADDR
    email = event['email']
    
    info = w_table.get_item(Key={"id": wid})
    profile_info = user_table.get_item(Key={"email": email})['Item']
    
    complete = False
    favorite = False
    if(wid in profile_info['doneids']):
        complete = True
    if(wid in profile_info['favoritedids']):
        favorite = True
    
    if 'Item' not in info:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': 'workout not found'
        }
    
    #info = info['Item']
    
    info['completed'] = complete
    info['favorited'] = favorite
    
    info['Item']['zip_code'] = int(info['Item']['zip_code'])
    print(info)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(info)
    }
