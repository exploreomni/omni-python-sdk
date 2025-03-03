from omni_python_sdk import OmniAPI
import csv, time

api_key = '<<your api key>>'
base_url = 'https://<<your omni host>>/'

# Initialize the API with your credentials
api = OmniAPI(api_key, base_url)


with open('users.csv', newline='') as csvfile:
    spamreader = csv.DictReader(csvfile)
    for row in spamreader:
        time.sleep(2)
        email = row.pop('email')
        displayName = row.pop('display_name')
        op = row.pop('op')
        if op == 'upsert':
            r1 = api.upsert_user(
                email=email,
                displayName=displayName,
                attributes=row
            )
        elif op == 'delete':
            api.delete_user(email)