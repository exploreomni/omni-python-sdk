from omni_python_sdk import OmniAPI

api = OmniAPI('https://<<your omni host>>','<<your api key>>')

response = api.generate_embed_url(
    {
    'contentPath': '/dashboards/example_metrics',
    'externalId': 'user@example.com',
    'name': 'Example (embed test user)',
    'secret': '<<your embed secret>>',
    'email':'user@example.com',
    'groups':'["my-example-group-name","second-group"]'
    }   
)
print(response.json()["url"])