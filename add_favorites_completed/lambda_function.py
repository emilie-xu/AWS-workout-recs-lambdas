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

#userid, favoritedids, doneids

def flatten(d, parent_key='', sep='_'):
    items = []
    for key, val in d.items():
        new_key = key
        if isinstance(val, collections.abc.MutableMapping):
            items.extend(flatten(val, new_key, sep=sep).items())
        else:
            items.append((new_key, val))
    return dict(items)

def floats_to_str(item,float_vars):
    for var in item:
        if var in float_vars:
            item[var]=Decimal(str(item[var]))
    return item

dict_list = []
var_list = ['userid', 'favoritedids', 'doneids']

def updateItems(workout, in_data, location_setting):
    #assuming input data in form (userid str, workoutid str, done bool, favorite bool)
    #print(in_data)
    try:
        rec2 = {}
        rec2['userid'] = in_data['userid']
        
        #NEED TO DO ERROR CHECKING FOR UNAVAILABLE
        userdata = table.get_item(
            Key={'id': userid}
            )
        
        favs = userdata['favoritedids']
        dones = userdata['doneids']
        
        if(in_data['favorite']==True):
            favs.append(in_data['workoutid'])
        if(in_data['done']==True):
            dones.append(in_data['workoutid'])
            
        rec2['favoritedids'] = favs
        rec2['doneids'] = dones
        rec2['latestUpdateTime'] = str(datetime.datetime.now())
        
        #dict_list.append(rec2)
        
        r = table.update_item(Item=rec2)
        #print('put record succeeded:', r)
        time.sleep(0.001)
    except Exception as e:
        print('add record failed: ', in_data)
        print(e)
  
        
def lambda_handler(event, context):
    
    # testing display on front-end
    print('event: ', json.dumps(event))
    
    #EXTRACT USER DATA OF FORM (userid str, workoutid str, done bool, favorite bool)
    
    userdataupdates = event#EXTRACT
    updateItems(userdataupdates)

    return {
        'status': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            'Content-Type': 'application/json'
        },
        'body': 'successfully updated favorite and done data'
    }