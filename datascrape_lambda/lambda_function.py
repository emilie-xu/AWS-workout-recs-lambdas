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

API_HOST = 'https://maps.googleapis.com/maps/api/place/textsearch/json?'
API_KEY = 'XXXXXXXXXXXXXXXXXXXX'

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('workout_database')

def flatten(d, parent_key='', sep='_'):
    items = []
    for key, val in d.items():
        new_key = key
        if isinstance(val, collections.abc.MutableMapping):
            items.extend(flatten(val, new_key, sep=sep).items())
        else:
            items.append((new_key, val))
    return dict(items)

list_float=['rating', 'lat', 'lng']

def floats_to_str(item,float_vars):
    for var in item:
        if var in float_vars:
            item[var]=Decimal(str(item[var]))
    return item
    
def get_results(query, pageToken):
    try:
        params = {
            'location': '40.805869 ,-73.964672',
            'query': query,
            'radius': 50000,
            'region': 'us',
            #'type':'gym',
            'key': API_KEY,
            'pageToken': pageToken
        }
        req = requests.get(API_HOST, params=params)
        #print(req.json())
        token = req.json().get('next_page_token')
        results = req.json().get('results')
        return token, results
    except Exception as e:
        print('query failed: ', query)
        print(e)

dict_list = []
var_list = ['place_id', 'name', 'rating', 'lat', 'lng', 'formatted_address', 'photos']

def addItems(workout, data, location_setting):
    for rec in data:
        try:
            #print('rec',rec)
            rec2 = {}
            rec2['place_id'] = rec['place_id']
            rec2['name'] = rec['name']
            rec2['lat'] = rec['lat']
            rec2['lng'] = rec['lng']
            rec2['rating'] = rec['rating']
            addr = rec['formatted_address']
            rec2['full_address'] = addr
            
            rec2['state'] = addr[-8:-6]
            rec2['zip'] = int(addr[-5:])

            rec2['location_setting'] = location_setting
            rec2['insertedAtTimestamp'] = str(datetime.datetime.now())
            
            if('photos' in rec):
                rec2['web'] = rec['photos'][0]['html_attributions'][0].split('"')[1]
            rec2['type'] = workout
            #print('rec2:', json.dumps(rec2))
            
            #print('data:',rec2)
            dict_list.append(rec2)
            
            r = table.put_item(Item=rec2)
            #print('put record succeeded:', r)
            time.sleep(0.001)
        except Exception as e:
            print('add record failed: ', rec)
            print(e)


def scrape():
    data_limit = 100
    types = ['cardio', 'strength training', 'yoga', 'pilates', 'trails']
    for workoutType in types:
        print('workoutType:', workoutType)
        batch_dict = []
        i = 0
        pageToken = ''
        if(workoutType != 'trails'):
            while(i<data_limit):
                print('processing batch entry',i)#, pageToken)
                time.sleep(0.1)
                pageToken, results = get_results(workoutType + ' workout class', pageToken)
                for r in results:
                    #print(r)
                    data = flatten(r)
                    data = floats_to_str(data, list_float) #data
                    data2 = {}
                    for k in var_list:
                        if str(k) in data:
                            data2.update({k: data[str(k)]})
                    batch_dict.append(data2)
                addItems(workoutType, batch_dict, 'gym')
                i += 20
                
        else:
             while(i<40):
                print('processing batch entry',i)#, pageToken)
                time.sleep(0.1)
                pageToken, results = get_results(workoutType, pageToken)
                for r in results:
                    #print(r)
                    data = flatten(r)
                    data = floats_to_str(data, list_float) #data
                    #print(data)
                    data2 = {}
                    for k in var_list:
                        if str(k) in data:
                            data2.update({k: data[str(k)]})
                    batch_dict.append(data2)
                addItems('cardio', batch_dict, 'outdoors')
                i+=20


#From stack overflow
def payload_constructor(data,action):
    action_string = json.dumps(action) + "\n"
    payload_string=""
    for datum in data:
        payload_string += action_string
        this_line = json.dumps(datum) + "\n"
        payload_string += this_line
    return payload_string

def create_indices(client):
    index_body = {
      'settings': {
        'index': {
          'number_of_shards': 1
        }
      }
    }
    response = client.indices.create('courses', body=index_body)
    print(response)
    print('Index creation successful')
    
def lambda_handler(event, context):
    
    # testing display on front-end
    print('event: ', json.dumps(event))
    
    scrape()
    
    #OPENSEARCH
    host = 'search-workouts-XXXXXXXXXXXXXXXXX.us-east-1.es.amazonaws.com'
    service = 'es'
    awsauth_opensearch = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
    
    client = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth_opensearch,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    #create_indices(client)
    
    print('Connected Opensearch')
    
    # Get opensource dictionary
    headers = { "Content-Type": "application/json" }
    opensearch_list = []
    for entry in dict_list:
        print('entry:',entry) 
        workoutType = entry.get("type")
        location = entry.get("location_setting")
        place_id = entry.get("place_id")
        zip = entry.get("zip")
        
        opensearch_dict = {
            'objectKey':place_id,
            'type': workoutType,
            'location': location,
            'zip': zip
        }
        
        url = 'https://'+ host + '/courses/_doc'
        r = requests.post(url, auth=awsauth_opensearch, data=json.dumps(opensearch_dict), headers=headers)
        #print(r.text)
        print(opensearch_dict)
        opensearch_list.append(opensearch_dict)
        
        
    action={
        "index": {
            "_index": 'courses'
        }
    }
    
    print(len(dict_list))
    # actions_body = payload_constructor(opensearch_list,action)
    # print(actions_body)
    
    #write into json file to use for opensearch
    # file = open("data.json", "w")
    # file.write(str(actions_body))
    # file.close()

    return {
        'status': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            'Content-Type': 'application/json'
        },
        'body': {'images': json.dumps(event)}
    }
    