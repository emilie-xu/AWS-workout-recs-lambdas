import base64
import json
from datetime import *
import boto3
import re
import inflection
import requests
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import time
import random

region = 'us-east-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

API_HOST_categories = 'https://data.cityofnewyork.us/resource/xtsw-fqvh.json'
API_HOST_events = 'https://data.cityofnewyork.us/resource/fudw-fgrp.json'
API_HOST_locations = 'https://data.cityofnewyork.us/resource/cpcm-i88g.json'
API_HOST_links = 'https://data.cityofnewyork.us/resource/ridc-7qqg.json'

APP_TOKEN = 'XXXXXXXXXXXXXXXX'

API_KEY = 'XXXXXXXXXXXXXX'

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('workouts')
print('connected to dynamodb')

es_endpoint = 'search-workouts-XXXXXXXXXXXXXXXXXX.us-east-1.es.amazonaws.com'

service = 'es'
awsauth_opensearch = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
headers = { "Content-Type": "application/json" }
url = 'https://'+ es_endpoint + '/wid/_doc'

client = OpenSearch(
    hosts = [{'host': es_endpoint, 'port': 443}],
    http_auth = awsauth_opensearch,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)
print('Connected Opensearch')

def getPidAddr(placeName):
    API_HOST = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input='
    placename = ('nyc ' + placeName).replace(' ', '%20')
    fields = ['formatted_address','place_id']
    outentries = '&inputtype=textquery&fields='
    for i in range(len(fields)-1):
        outentries += (fields[i]+'%2C')
    outentries += fields[len(fields)-1]
    outentries += '&key='
    url = API_HOST+placename+outentries+API_KEY
    response = requests.request("GET", API_HOST+placename+outentries+API_KEY, headers={}, data={})
    resp = response.json()['candidates'][0]
    return resp['place_id'], resp['formatted_address']

def getBusinessHours(pid):
    url = 'https://maps.googleapis.com/maps/api/place/details/json?placeid='+pid+'&fields=opening_hours&key=' + API_KEY
    response = requests.request("GET", url, headers={}, data={})
    resp = response.json()['result']['opening_hours']
    return resp['periods'], resp['weekday_text']
    
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
    
#create_indices(client)
    
def changeVarVal(description, key_arr, var, varval):
    lowercase_desc = description.lower()
    for k in key_arr:
        if k in lowercase_desc:
            var = varval
            print('var changed to', varval)
            return var
            break
    return var
    
TAG_RE = re.compile(r'<[^>]+>')
APOS = ["&rsquo;", "&#39;", "&quot;", "&ldquo;", "&rdquo;"]
def remove_tags(text):
    t = str(TAG_RE.sub('', text))
    for a in APOS:
        t = t.replace(a, "'")
    t = t.replace("&nbsp;"," ").replace("&amp;", "&").replace("&ndash;", "-").replace('\t', ' ')
    while "  " in t:
        t = t.replace("  ", " ")
    return t

def getWeekDay(datestring):
    dt = datetime.strptime(datestring, '%Y-%m-%dT%H:%M:%S.000')
    date = datetime(dt.year,dt.month, dt.day).date()
    return date.strftime('%A'), dt.month, dt.day
                
def lambda_handler(event, context):
    
    print('event: ', json.dumps(event))
    
    param_categories = ['Fitness', 'Outdoor Fitness']
    
    all_entry_names = set()
    
    for category in param_categories:
        params = {'name': category}
        
        
        req = requests.get(API_HOST_categories, params=params)
        #print(req.text)
        
        matched = []
        startidx = 0
        if category == "Fitness":
            startidx = 106
        j = 0
        for i in range(startidx, len(req.json())):
            print(i)
            if(i%5==0):
                time.sleep(0.001)
            
            r = req.json()[i]
            eid = r['event_id']
            p = {'event_id': eid}
            
            e_data = requests.get(API_HOST_events, params=p)
            if(e_data.json()==[]):
                continue;
            #print('has e_data')
            descr = e_data.json()[0]
            if(descr['title'] in all_entry_names):
                continue
            else:
                all_entry_names.add(descr['title'])
                
            loc_data = requests.get(API_HOST_locations, params=p)
            if(loc_data.json()==[]):
                continue;
            #print('has loc_data')
            site_data = requests.get(API_HOST_links, params=p)
            if(site_data.json()==[]):
                continue;
            #print('has site_data')
            
            #print(e_data.json(), loc_data.json())
            
            try:
                #descr = e_data.json()[0]
                loc = loc_data.json()[0]
                site = site_data.json()[0]
                
                #print(descr, loc, site)
            
                datestring = descr['date']
                #print(datestring)
                weekday, month, day = getWeekDay(datestring)
                
                date = str(datetime(2023,month, day).date())
                if(date<str(datetime.now())):
                    date = str(datetime(2024,month, day).date())
                
                workout_id = 'nyc' + str(descr['event_id'])
                title = descr['title']
                
                if 'cancelled' in title.lower():
                    print('event cancelled': title)
                    continue
                
                description = remove_tags(descr['description'])
                print('descr:',description)
            
                summary = descr['snippet']
                start_time = descr['start_time']
                end_time = descr['end_time']
                #img_url
                
                addrName = loc['name'] 
                addr = ''
                if 'address' in loc:
                    addr = loc['address'] #also has lat, long, zip
                else: #GET GOOGLE MAPS DATA
                    pid, addr = getPidAddr(addrName)
                city = 'New York'
                state = 'NY'
                zip_code = 00000
                if 'zip' in loc:
                    zip_code = loc['zip']
                longitude = loc['long']
                lat = loc['lat']
                website = site['link_url']
                
                location = 'gym'
                if category == "Outdoor Fitness":
                    location = 'outdoors'
                else:
                    outdoor_keys = ['park', 'outdoors', 'nature']
                    location = changeVarVal(description, outdoor_keys, location, 'outdoors')
                
                intensity = 'moderate'
                lowimpact_keys = ['walk', 'low-impact', 'low impact', 'swim', 'low intensity', 'low-intensity', 'tai chi', 'taichi', 'hatha']
                intensity = changeVarVal(description, lowimpact_keys, intensity, 'low')
                highimpact_keys = ['high-impact', 'high impact', 'boxing', 'high intensity', 'high-intensity']
                intensity = changeVarVal(description, highimpact_keys, intensity, 'high')
                
                w_type = 'fitness'
                
                keys = ['pilates', 'yoga', 'cycling', 'biking', 'bike', 'swim', 'swimming', 'cardio']
                for k in keys:
                    if k in title.lower():
                        w_type = k
                        print('var changed to', k)
                        break;
                    elif k in description.lower():
                        w_type = k
                        print('var changed to', k)
                        break;
                if w_type in ['biking', 'bike']:
                    w_type = 'cycling'
                elif w_type == 'swim':
                    w_type = 'swimming'
                    
                ev = {}
                ev['id']=workout_id
                ev['title']=title
                ev['description']=description
                #ev['summary']=summary
                ev['start_time']=start_time
                ev['end_time']=end_time
                ev['city']=city
                ev['state']=state
                ev['long']=longitude
                ev['lat']=lat
                ev['zip_code']=zip_code
                ev['website']=website
                ev['location']=location
                ev['intensity']=intensity
                ev['type'] = w_type
                ev['addr'] = addr
                ev['addrName'] = addrName
                ev['startDate'] = date
                ev['weekDay'] = weekday
                #ev['weekly'] = False
                
                print(ev)
                #all_entries.append(ev)
                    
                r = table.put_item(Item=ev)
                
                opensearch_dict = {
                    'wid':workout_id,
                    'type': w_type,
                    'intensity': intensity,
                    'location': location,
                    'zip': zip_code,
                    'date': date
                }
                print(opensearch_dict)
                
                r = requests.post(url, auth=awsauth_opensearch, data=json.dumps(opensearch_dict), headers=headers)
                print(r.text)
    
            except Exception as e:
                print('add record failed: ', descr)
                print(e)
        
                
    return {
        'status': 200,
        'body': {'uploadComplete': True}
    }
    
