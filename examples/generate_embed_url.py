from omni_python_sdk import AuthenticatedClient

client = AuthenticatedClient(base_url='https://<<your omni host>>', token='<<your api key>>')

# The /embed/sso/generate-url endpoint is not part of the public OpenAPI spec,
# so call it through the client's underlying httpx client (auth and base_url
# are already configured).
response = client.get_httpx_client().post(
    '/embed/sso/generate-url',
    json={
        'contentPath': '/dashboards/example_metrics',
        'externalId': 'user@example.com',
        'name': 'Example (embed test user)',
        'secret': '<<your embed secret>>',
        'email': 'user@example.com',
        'groups': '["my-example-group-name","second-group"]'
    }
)
response.raise_for_status()
print(response.json()["url"])
