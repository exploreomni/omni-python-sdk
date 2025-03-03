from omni_python_sdk import OmniAPI

api_key = '<<your api key>>'
base_url = '<<your omni host>>'

# Initialize the API with your credentials
api = OmniAPI(api_key, base_url)

# retrieve the dashboard
dashboard_export = api.document_export('<<dashboard identifier>>')
# change the dashboard model id
dashboard_export.update({'baseModelId':'<< model id of new location >>'})
# import the modified document
api.document_import(dashboard_export)