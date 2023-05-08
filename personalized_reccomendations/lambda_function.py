import json
import boto3
from boto3.dynamodb.conditions import Key
import math
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def weighted_dot_product(vec1, vec2, weights):
    #print('weighted dot prod: ')
    # for a, b, w in zip(vec1, vec2, weights):
    #     print(a, b, w)
    #     print(a*b*w)
    #print('final result:', sum(a * b * w for a, b, w in zip(vec1, vec2, weights)))
    return sum(a * b * w for a, b, w in zip(vec1, vec2, weights))

def weighted_magnitude(vec, weights):
    #print('weighted mag', math.sqrt(weighted_dot_product(vec, vec, weights)))
    return math.sqrt(weighted_dot_product(vec, vec, weights))

def weighted_cosine_similarity(vec1, vec2, weights):
    #print('weighted cosine similarity', weighted_dot_product(vec1, vec2, weights) / (weighted_magnitude(vec1, weights) * weighted_magnitude(vec2, weights)))
    return weighted_dot_product(vec1, vec2, weights) / (weighted_magnitude(vec1, weights) * weighted_magnitude(vec2, weights))

def time_to_minutes(time_str):
    hours, minutes = map(int, time_str.split(':'))
    return hours * 60 + minutes

def one_hot_encoding(item, options):
    return [1 if item == option else 0 for option in options]

def getRollingWindowWorkoutIds(email):
    displayNum = 3
    
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("user-profile")
    info = table.get_item(Key={"email": email})
    if 'Item' not in info:
        return []
    favs = info['Item']['favoritedids']
    dones = info['Item']['doneids']
    
    returnedfavs = []
    returnedcompletes = []

    if(len(favs)<displayNum):
        returnedfavs = favs
    else:
        returnedfavs = favs[-displayNum:]
    if(len(returnedcompletes)<displayNum):
        returnedcompletes = dones
    else:
        returnedcompletes = dones[-displayNum:]
    
    return returnedcompletes, returnedfavs

def getWorkoutInfo(widArr):
    if(widArr==[]):
        return []
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("workouts")
    info = []
    for wid in widArr:
        #MIGHT NEED TO ADD ERROR CHECKING
        entry = table.get_item(Key={"id": wid})['Item']
        info.append(entry)
    return info
    
def lambda_handler(event, context):
    
    email = event["email"]
    
    # Retrieve user's past 3 completed workouts and past 3 favorited workout info
    completeIds, favIds = getRollingWindowWorkoutIds(email)
    completedInfo = getWorkoutInfo(completeIds)
    favoritedInfo = getWorkoutInfo(favIds)
    print(completedInfo, favoritedInfo)
    
    # Retrieve user preferences
    dynamodb = boto3.resource("dynamodb")
    user_profile_table = dynamodb.Table("user-profile")
    response = user_profile_table.query(KeyConditionExpression=Key("email").eq(email))
    user_preferences = response["Items"][0]
    
    intensity_mapping = {
        'low': 1,
        'moderate': 2,
        'high': 3
    }
    user_intensity = intensity_mapping[user_preferences["intensity"].lower()]/3
    user_time_of_day = time_to_minutes(user_preferences["time_of_day"])/2400
    user_type_encoding = one_hot_encoding(user_preferences["fav_workout_type"][0].lower(), ['cardio', 'fitness', 'pilates', 'yoga', 'cycling' , 'swimming'])
    
    print()
    
    print('intensity', user_intensity)
    print('time_of_day', user_time_of_day)
    print('type one hot', user_type_encoding)
    
    # Update user_type_encoding weights based on completed and favorited workouts
    for workout in completedInfo + favoritedInfo:
        workout_type = workout["type"].lower()
        if workout_type in ['cardio', 'fitness', 'pilates', 'yoga', 'cycling', 'swimming']:
            index = ['cardio', 'fitness', 'pilates', 'yoga', 'cycling', 'swimming'].index(workout_type)
            user_type_encoding[index] += 1
    
    
    user_vector = [user_intensity, user_time_of_day] + user_type_encoding
    print('user_vect', user_vector)
    
    weights = [2, 1] + [3] * len(user_type_encoding)
    
    print('weights', weights)
    # Retrieve workouts
    workout_table = dynamodb.Table("workouts")
    response = workout_table.scan()
    workouts = response["Items"]
    
    # Calculate cosine similarities
    similarities = []
    for workout in workouts:
        workout_intensity = intensity_mapping[workout["intensity"].lower()]/3
        workout_start_time = time_to_minutes(workout["start_time"])/2400
        workout_type_encoding = one_hot_encoding(workout["type"].lower(), ['cardio', 'fitness', 'pilates', 'yoga', 'cycling' , 'swimming'])

        workout_vector = [workout_intensity, workout_start_time] + workout_type_encoding
        print('workout_vect', workout_vector)
        print('user_vect', user_vector)
        
        similarity = weighted_cosine_similarity(user_vector, workout_vector, weights)
        
        print(similarity)
        similarities.append((workout, similarity))
    
    # Sort workouts by similarity (descending order) and select top 10 or total entries
    displayLength = min(15, len(similarities))
    recommendations = sorted(similarities, key=lambda x: x[1], reverse=True)[:displayLength]
    
    # Extract workout details from recommendations
    #recommendation_details = [rec[0] for rec in recommendations]
    
    return {
        "statusCode": 200,
        "body": json.dumps(recommendations, cls=DecimalEncoder)
    }
