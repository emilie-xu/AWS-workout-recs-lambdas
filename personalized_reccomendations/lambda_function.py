
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
HOST = 'search-workouts-4i6jauwrslwk3mj5ipv6sghbyi.us-east-1.es.amazonaws.com'
INDEX = 'courses'

def to_json_parsable(d):
    #print(d)
    for k, v in d.items():
        if(type(v) == Decimal):
            #print(v)
            d[k] = float(v)
            #print(d[k])

def parse_query(query_body):
    uid = query_body['userid']
    workoutType = query_body['type']
    level = query_body['level']
    location = query_body['location']
    intensity = query_body['intensity']
    time = query_body['time']
    duration = query_body['duration']
    zipcode = 10027
    
    for g in query_body['goals']:
        t=''
        if g=='weight loss':
            t = 'cardio'
        elif g=='cardio':
            t = 'cardio'
        elif g=='muscle':
            t = 'strength'
        elif g=='fitness':
            print('fitness')#t = 'all'
        else:
            print('unknown goal:', g)
        if t not in workoutType:
            workoutType.append(t)
    print(workoutType)
    return [uid, workoutType, zipcode, level, location, intensity, time, duration]
    
def get_courseIDs(slots):
    #assume querybody is type for now
    uid, workoutType, zipcode, level, location, intensity, time, duration = slots
    print(workoutType, zipcode)
    #ALSO CAN ADD OUTDOORS/INDOORS, STATE
    
    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
    http_auth=get_awsauth(REGION, 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)
    
    results = []
    
    for t in workoutType:
        q = {'size': 10,
                "query": {
                    "dis_max": {
                      "queries": [
                        { "match": { "type": t }},
                        { "match": { "zip": zipcode }}
                      ]
                    }
                  }
            }
        
        res = client.search(index=INDEX, body=q)
        print('query successful:',res)
    
        hits = res['hits']['hits']
       
        for hit in hits:
            entry = hit['_source']
            print('entry:', entry)
            results.append(entry['objectKey'])
    return results

def workout_info(place_ids):
    all_info = []
    
    client = boto3.resource('dynamodb', region_name = REGION)
    table = client.Table('workout_database')
    #print(table)
    for pid in place_ids:
        try:
            info = table.query(
                KeyConditionExpression=Key('place_id').eq(pid)
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
    user_info = event["multiValueQueryStringParameters"]
    #print("query:",querybody)
    
    parsed_info = parse_query(user_info)
    
    
    recs = get_courseIDs(parsed_info)
    
    # choose 5 random fitness centers
    max_len = len(recs)
    random_pids = random.sample(recs, min(5, max_len))
    
    
    # get info from dynamo
    w_info = workout_info(random_pids)
    
    print('workout info:', w_info)
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

