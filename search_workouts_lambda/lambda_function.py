import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from decimal import Decimal

import random
from boto3.dynamodb.conditions import Key
#from botocore.vendored import requests

REGION = 'us-east-1'
HOST = 'search-workouts-XXXXXXXXXXXXXXXX.us-east-1.es.amazonaws.com'
INDEX = 'courses'

def to_json_parsable(d):
    #print(d)
    for k, v in d.items():
        if(type(v) == Decimal):
            #print(v)
            d[k] = float(v)
            #print(d[k])
        
def parse_query(query_body):
    client = boto3.client('lexv2-runtime')
    response = client.recognize_text(
        botId='XXXXXX', # MODIFY HERE
        botAliasId='XXXXXXX', # MODIFY HERE
        localeId='en_US',
        sessionId='testuser',
        text=query_body)
    print('Response:', response)
    
    lex_resp = response['sessionState']['intent']['slots']
    
    t, z = '', ''
    if(lex_resp['type']!=None):
        t = lex_resp['type']['value']['interpretedValue']
    if(lex_resp['zip']!=None):
        z = lex_resp['zip']['value']['interpretedValue']
    resp_arr = [t, z]
    #resp_arr = [lex_resp['type']['value']['interpretedValue'], lex_resp['zip']['value']['interpretedValue']]
    print(resp_arr)
    return resp_arr
    
def get_courseIDs(slots):
    #assume querybody is type for now
    workoutType, zipcode = slots
    print(workoutType, zipcode)
    #ALSO CAN ADD OUTDOORS/INDOORS, STATE
    q = {'size': 20,
            "query": {
                "bool": {
                  "should": [
                    { "match": { "type": workoutType }},
                    { "match": { "zip": zipcode }}
                  ]
                }
              }
        }
    
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
    http_auth=get_awsauth(REGION, 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)
    
    res = client.search(index=INDEX, body=q)
    print('query successful:',res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        try:
            entry = hit['_source']
            print('entry:', entry)
            results.append(entry['objectKey'])
        except:
            try:
                entry = hit['_source']
                print('entry:', entry)
                results.append(entry['id'])
            except Exception as e:
                print('append hit entry failed:', hit['_source'])
                print(e)
    return results

def workout_info(place_ids):
    all_info = []
    
    client = boto3.resource('dynamodb', region_name = REGION)
    table = client.Table('workouts')
    #print(table)
    for pid in place_ids:
        try:
            info = table.query(
                KeyConditionExpression=Key('id').eq(pid)
            )
            #print("info:", info['Items'])
            
            indiv_info = info['Items'][0]
            to_json_parsable(indiv_info)
            print('query success:', indiv_info)
            
            all_info.append(indiv_info)
        except Exception as e:
            print(e)
            print('query failed:', pid)
    
    return all_info

def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event))

    ##IMPLEMENT ERROR HANDLING##
    if(event == [] or event =='' or event =={}):
        return {
            'statusCode': 200,
            'headers': {
                "Access-Control-Allow-Origin": "*",
                'Content-Type': 'application/json'
            },
            'body': ''
        }
    
    # Get input query from API Gateway
    querybody = event["queryStringParameters"]["query"]
    #print("query:",querybody)
    
    #decoded_query = urllib.parse.unquote_plus(querybody)
    
    parsed_info = parse_query(querybody)
    recs = get_courseIDs(parsed_info)
    
    # choose 10 random fitness centers
    max_len = len(recs)
    random_pids = random.sample(recs, min(10, max_len))
    print(random_pids)
    
    # get info from dynamo
    w_info = workout_info(random_pids)
    
    print('workout info:', w_info[0])
    return {
        'statusCode': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            'Content-Type': 'application/json'
        },
        'body': json.dumps({'workouts': w_info})
    }

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)