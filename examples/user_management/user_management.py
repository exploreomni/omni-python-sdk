from omni_python_sdk import AuthenticatedClient
from scim_helpers import upsert_user, delete_user
import csv, time

api_key = '<<your api key>>'
base_url = 'https://<<your omni host>>'


# Initialize the client with your credentials
client = AuthenticatedClient(base_url=base_url, token=api_key)

with open('users.csv', newline='') as csvfile:
    spamreader = csv.DictReader(csvfile)
    for row in spamreader:
        time.sleep(2)
        email = row.pop('email')
        displayName = row.pop('display_name')
        op = row.pop('op')
        if op == 'upsert':
            upsert_user(
                client,
                email=email,
                display_name=displayName,
                attributes=row
            )
        elif op == 'delete':
            delete_user(client, email)
