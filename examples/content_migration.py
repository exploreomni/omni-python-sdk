from omni_python_sdk import AuthenticatedClient
from omni_python_sdk.api.unstable import unstable_documents_export, unstable_documents_import
from omni_python_sdk.models import DocumentImportBody

api_key = '<<your api key>>'
base_url = '<<your omni host>>'

# Initialize the client with your credentials
client = AuthenticatedClient(base_url=base_url, token=api_key)

# retrieve the dashboard
dashboard_export = unstable_documents_export.sync(
    '<<dashboard identifier>>', client=client
).to_dict()
# change the dashboard model id
dashboard_export.update({'baseModelId': '<< model id of new location >>'})
# import the modified document
unstable_documents_import.sync(client=client, body=DocumentImportBody.from_dict(dashboard_export))
