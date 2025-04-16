from omni_python_sdk import OmniAPI
import csv, time

api_key = '<<your api key>>'
base_url = 'https://<<your omni host>>'


# Initialize the API with your credentials
api = OmniAPI(api_key, base_url)

with open('user_groups.csv', newline='') as csvfile:
    user_groups = csv.DictReader(csvfile)
    for row in user_groups:
        time.sleep(2)
        email = row.pop('email')
        op = row.pop('op')
        if op == '+':
            user = api.return_user_by_email(email)
            api.add_user_to_group(
                row['group_name'],
                user['id']
            )
        elif op == '-':
            user = api.return_user_by_email(email)
            api.remove_user_from_group(
                row['group_name'],
                user['id']
            )