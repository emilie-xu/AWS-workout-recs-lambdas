'''
input:
{email: str,
workoutid: str,
toggleDone: bool,
toggleFavorite: bool}

output:
{email: str,
workoutid: str,
done: bool,
favorite: bool,
doneChanged: bool,
favChanged: bool
}
'''
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user-profile')

dict_list = []
var_list = ['userid', 'favoritedids', 'doneids']

def updateItems(in_data):
    #assuming input data in form (userid str, workoutid str, toggleDone bool, toggleFavorite bool)
    #print(in_data)
    try:
        rec2 = {}
        email = in_data['email']
        rec2['email'] = email
        
        #NEED TO DO ERROR CHECKING FOR UNAVAILABLE
        userdata = table.get_item(Key={"email": email})['Item']
        
        print(userdata)
        
        favs = userdata['favoritedids']
        dones = userdata['doneids']
        wid = in_data['workoutid']
        
        toggleDone = in_data['done']
        toggleFavorite = in_data['favorite']
        
        done = wid in dones
        favorite = wid in favs
        
        if(toggleFavorite):
            if(wid not in favs):
                favs.append(wid)
                favorite = True
            else:
                favs.remove(wid)
                favorite = False
        if(toggleDone):
            if(wid not in dones):
                dones.append(wid)
                done = True
            else:
                dones.remove(wid)
                done = False

        userdata['favoritedids'] = favs
        userdata['doneids'] = dones
        userdata['latestUpdateTime'] = str(datetime.datetime.now())
        
        #dict_list.append(rec2)
        print(userdata)
        
        r = table.put_item(Item=userdata)#, Key=email)
        print('put record succeeded:', r)
        return favorite, done
    except Exception as e:
        print('add record failed: ', in_data)
        print(e)
  
        
def lambda_handler(event, context):
    
    # testing display on front-end
    print('event: ', json.dumps(event))
    
    #EXTRACT USER DATA OF FORM (userid str, workoutid str, done bool, favorite bool)
    
    userdataupdates = event#EXTRACT
    fav, done = updateItems(userdataupdates) 
    
    userdataupdates['favChanged'] = userdataupdates['favorite']
    userdataupdates['doneChanged'] = userdataupdates['done']
    userdataupdates['favorite'] = fav
    userdataupdates['done'] = done
    print(userdataupdates)
    
    return {
        'status': 200,
        'headers': {
            "Access-Control-Allow-Origin": "*",
            'Content-Type': 'application/json'
        },
        'body': json.dumps(userdataupdates)
    }