from omni_python_sdk import AuthenticatedClient
from scim_helpers import return_user_by_email, add_user_to_group, remove_user_from_group
import csv, time

api_key = '<<your api key>>'
base_url = 'https://<<your omni host>>'


# Initialize the client with your credentials
client = AuthenticatedClient(base_url=base_url, token=api_key)

with open('user_groups.csv', newline='') as csvfile:
    user_groups = csv.DictReader(csvfile)
    for row in user_groups:
        time.sleep(2)
        email = row.pop('email')
        op = row.pop('op')
        if op == '+':
            user = return_user_by_email(client, email)
            add_user_to_group(
                client,
                row['group_name'],
                user['id']
            )
        elif op == '-':
            user = return_user_by_email(client, email)
            remove_user_from_group(
                client,
                row['group_name'],
                user['id']
            )
