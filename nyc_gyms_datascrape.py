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
import random

API_HOST = 'https://maps.googleapis.com/maps/api/place/textsearch/json?'
API_KEY = 'XXXXXXXXXXXXXXX'

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('workouts')

all_entry_ids = set()

def flatten(d, parent_key='', sep='_'):
    items = []
    for key, val in d.items():
        new_key = key
        if isinstance(val, collections.abc.MutableMapping):
            items.extend(flatten(val, new_key, sep=sep).items())
        else:
            items.append((new_key, val))
    return dict(items)

list_float=['lat', 'lng']

def floats_to_str(item,float_vars):
    for var in item:
        if var in float_vars:
            item[var]=str(item[var])
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
var_list = ['place_id', 'name', 'lat', 'lng', 'formatted_address'] #, 'photos']

def getBusinessInfo(pid):
    url = 'https://maps.googleapis.com/maps/api/place/details/json?placeid='+pid+'&fields=name%2Copening_hours/periods%2Cwebsite%2Ceditorial_summary&key=' + API_KEY
    response = requests.request("GET", url, headers={}, data={})
    return response.json()['result']
    #returns hours, website, summary (if exists)

def getIntensityDayDuration(arr):
    idx = random.randint(0, len(arr)-1)
    return arr[idx]
    
def getTime(start, end):
    print(start, end)
    hr = random.randint(int(start[:2]), int(end[:2])-1)
    mins = random.randint(0,1)*30
    starttime = datetime.datetime(2000, 1, 1, hr, mins)
    duration = getIntensityDayDuration([45, 60, 75, 90])
    endtime = starttime + datetime.timedelta(minutes=duration)
    return starttime.time().strftime("%H:%M"), endtime.time().strftime("%H:%M")

def getWorkoutTimes(periods, day):
    dayMapping = {
        'Sunday': 0,
        'Monday': 1,
        'Tuesday': 2,
        'Wednesday': 3,
        'Thursday': 4,
        'Friday': 5,
        'Saturday': 6
    }
    dayInt = dayMapping[day]
    hrs = periods[dayInt] #[day]
    opening_time = hrs['open']['time'][:2] + ':' + hrs['open']['time'][2:]
    closing_time = hrs['close']['time'][:2] + ':' + hrs['close']['time'][2:]
    if dayInt>0 and dayInt<6: #weekday
        if opening_time>'09:00' or opening_time=='09:00':
            if closing_time<'16:00' or closing_time=='16:00':
                return getTime(opening_time, closing_time)
            return getTime('16:00', closing_time)
        elif closing_time<='16:00' or closing_time=='16:00':
            return getTime(opening_time, '09:00')
        return getTime(opening_time, '09:00') if random.randint(0,1)==0 else getTime('16:00', closing_time)
    start, end = getTime(opening_time, closing_time)
    return start, end
    
def addItems(workout, data, location_setting):
    for rec in data:
        try:
            #print('rec',rec)
            
            rec2 = {}
            pid = rec['place_id']
            
            rec2['id'] = 'gym'+workout[0]+pid
            
            if(workout == 'strength training'):
                rec2['id'] = 'gymf'+pid
            
            if rec2['id'] in all_entry_ids:
                continue
            else:
                all_entry_ids.add(rec2['id'])
                
            info = getBusinessInfo(pid)
            if 'opening_hours' not in info:
                continue
            periods = info['opening_hours']['periods']
            
            addrName = rec['name']
            intensity = getIntensityDayDuration(['low', 'moderate', 'high'])
            
            #title
            if workout != 'trails':
                rec2['title'] = addrName + ': ' + intensity + ' intensity ' + workout
            else: 
                rec2['title'] = addrName + ': Biking, Walking, Hiking'
            #descr
            if 'editorial_summary' in info:
                rec2['description'] = info['editorial_summary']['overview']
            elif workout != 'trails':
                rec2['description'] = 'Join '+addrName+' for '+ intensity + ' intensity ' + workout + ' classes. No description available'
            #weekDay
            day = getIntensityDayDuration(['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'])
            rec2['weekDay'] = day
            #start_time
            start, end = getWorkoutTimes(periods, day)
            rec2['start_time'] = start
            rec2['end_time'] = end
            #website
            if 'website' in info:
                rec2['website'] = info['website']
            #intensity
            rec2['intensity'] = intensity
            #startDate
            rec2['startDate'] = 'unavailable'
            
            rec2['addrName'] = addrName
            rec2['lat'] = rec['lat']
            rec2['long'] = rec['lng']
            addr = rec['formatted_address']
            rec2['addr'] = addr
            
            addr_arr = addr.split(',')
            if 'USA' in addr:
                addr_arr = addr_arr[:-1]
                
            rec2['city'] = addr_arr[-2][1:]
            rec2['state'] = addr_arr[-1][1:3]
            rec2['zip_code'] = str(addr_arr[-1][-5:])
            #rec2['city'] = rec['formatted_address']
            rec2['location'] = 'gym'
            
            rec2['type'] = workout
            if workout == 'strength training':
                rec2['type'] = 'fitness'
            #print('rec2:', json.dumps(rec2))
            
            print('data:',rec2)
            
            
            r = table.put_item(Item=rec2)
            print('put record succeeded:', r)
            
            dict_list.append(rec2)
            
            time.sleep(0.0001)
        except Exception as e:
            print('add record failed: ', rec)
            print(e)


def scrape():
    
    data_limit = 60
    types = ['yoga', 'cycling' , 'swimming', 'trails', 'cardio', 'strength training', 'pilates', ]
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
                    if 'trail' in data['name'].lower():
                        continue
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
    response = client.indices.create('wid', body=index_body)
    print(response)
    print('Index creation successful')

def lambda_handler(event, context):
    
    # testing display on front-end
    print('event: ', json.dumps(event))
    
    scrape()
    
    #OPENSEARCH
    host = 'search-workouts-XXXXXXXX.us-east-1.es.amazonaws.com'
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
        wid = entry.get("id")
        zip_code = entry.get("zip")
        intensity = entry.get("intensity")
        
        opensearch_dict = {
            'wid':wid,
            'type': workoutType,
            'intensity': intensity,
            'location': location,
            'zip': zip_code
        }
        
        url = 'https://'+ host + '/wid/_doc'
        r = requests.post(url, auth=awsauth_opensearch, data=json.dumps(opensearch_dict), headers=headers)
        print(r.text)
        
        print(opensearch_dict)
        opensearch_list.append(opensearch_dict)
        
    
    print(len(dict_list))
    
    print(len(all_entry_ids))
    
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
        'body': json.dumps(event)
    }
    
