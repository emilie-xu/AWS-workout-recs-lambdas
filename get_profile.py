
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('user-profile')

#input: {email: string}
#output: {profile info}

def lambda_handler(event, context):
    # TODO implement
    print(event)
    
    email = event['email']
    
    print(email)
    info = table.get_item(Key={"email": email})
    print(info)
    if 'Item' not in info:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': 'no profile data'
        }
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(info['Item'])
    }