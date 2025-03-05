import requests
import pyarrow as pa
import pyarrow.ipc as ipc
import io
import urllib.parse
import json
import ndjson
import base64
from typing import List, Tuple, Any

class OmniAPI:
    def __init__(self, api_key: str, base_url: str = "https://dev.thundersalmon.com/api/unstable"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

    def wait_query_blocking(self, remaining_job_ids:  List[str]) -> Tuple[Any, bool]:
        url = f"{self.base_url}/query/wait"
        
        # URL encode the query parameter
        encoded_query = urllib.parse.urlencode({'job_ids': json.dumps(remaining_job_ids)})
        response = requests.get(f"{url}?{encoded_query}", headers=self.headers)
        
        if response.status_code == 200:
            # Parse NDJSON response
            response_json = ndjson.loads(response.text)
            footer = response_json[-1]
            done = footer['timed_out'] == 'false'
            return response_json, done
        else:
            response.raise_for_status()
    
    def run_query_blocking(self, body: dict):
        url = f"{self.base_url}/query/run"
        response = requests.post(url, headers=self.headers, json=body)
    
        if response.status_code == 200:
            # Parse NDJSON response
            response_json = ndjson.loads(response.text)
            footer = response_json[-1]
            done = footer['timed_out'] == 'false'
            while not done:
                response_json, done = self.wait_query_blocking(footer['remaining_job_ids'])
            data_payload = next((data_payload for data_payload in response_json if "result" in data_payload), None)
            if data_payload is not None:
                base64_data = data_payload['result']
                raw_arrow_data = base64.b64decode(base64_data)
                # Read Arrow table from raw data
                buffer = io.BytesIO(raw_arrow_data)
                reader = ipc.open_stream(buffer)
                table = reader.read_all()
                return table, data_payload['summary']['fields']
            else:
                raise ValueError("No result found in the response.")
        else:
            response.raise_for_status()
    
    def base_model_url(self):
        return f"{self.base_url}/model"
    
    def model_url(self, model_id: str):
        return f"{self.base_model_url()}/{model_id}"
    
    def base_topic_url(self, model_id: str):
        return f"{self.base_model_url()}/{model_id}/topic"
    
    def topic_url(self, model_id: str, topic_name: str):
        return f"{self.base_topic_url(model_id)}/{topic_name}"

    def base_view_url(self, model_id: str):
        return f"{self.base_model_url()}/{model_id}/view"

    def view_url(self, model_id: str, view_name: str):
        return f"{self.base_view_url(model_id)}/{view_name}"

    def base_field_url(self, model_id: str):
        return f"{self.base_view_url(model_id)}/field"
    
    def field_url(self, model_id: str, view_name: str, field_name: str):
        return f"{self.view_url(model_id, view_name)}/field/{field_name}"

    def create_model(self, connection_id: str, body: dict):
        url = self.base_model_url()
        body["connectionId"] = connection_id
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()

    def create_topic(self, model_id: str, base_view_name: str, body: dict):
        url = self.base_topic_url(model_id)
        body["baseViewName"] = base_view_name
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()

    def update_topic(self, model_id: str, topic_name: str, body: dict):
        url = self.topic_url(model_id, topic_name)
        response = requests.patch(url, headers=self.headers, json=body)
        return response.json()

    def delete_topic(self, model_id: str, topic_name: str):
        url = self.topic_url(model_id, topic_name)
        response = requests.delete(url, headers=self.headers)
        return response.json()

    def create_view(self, model_id: str, view_name: str, body: dict):
        url = self.base_view_url(model_id)
        body["viewName"] = view_name
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()
    
    def update_view(self, model_id: str, view_name: str, body: dict):
        url = self.view_url(model_id, view_name)
        body["viewName"] = view_name
        response = requests.patch(url, headers=self.headers, json=body)
        return response.json()
    
    def delete_view(self, model_id: str, view_name: str):
        url = self.view_url(model_id, view_name)
        response = requests.delete(url, headers=self.headers)
        return response.json()

    def create_field(self, model_id: str, view_name: str, field_name: str, body: dict):
        url = self.base_field_url(model_id)
        body["fieldName"] = field_name
        body["viewName"] = view_name
        response = requests.post(url, headers=self.headers, json=body)
        return response.json()

    def update_field(self, model_id: str, view_name: str, field_name: str, body: dict):
        url = self.field_url(model_id, view_name, field_name)
        response = requests.patch(url, headers=self.headers, json=body)
        return response.json()

    def delete_field(self, model_id: str, view_name: str, field_name: str):
        url = self.field_url(model_id, view_name, field_name)
        response = requests.delete(url, headers=self.headers)
        return response.json()
    
    def create_user(self, body:dict):
        url = f"{self.base_url}/api/scim/v2/users"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, json=body)
        return response
    
    def update_user(self, id, body:dict):
        url = f"{self.base_url}/api/scim/v2/users/{id}"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.put(url, headers=headers, json=body)
        return response
    
    def find_user_by_email(self, email:str):
        url = f"{self.base_url}/api/scim/v2/users"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, headers=headers, params={'filter': f'userName eq "{email}"'})
        return response
    
    def upsert_user(self, email:str, displayName:str, attributes:dict):
        body ={
            "urn:omni:params:1.0:UserAttribute":self.listify(attributes)
        }
        response = self.find_user_by_email(email)
        if response.status_code == 200:
            users = response.json()['Resources']
            if len(users) == 1:
                user = users[0]
                body.update({"userName":email, "displayName":displayName})
                update_response = self.update_user(user['id'],body)
                if update_response.status_code == 200:
                    print(f"updated user id {user['id']}")
                else:
                    print(f"Error ({update_response.status_code}) updating user id {user['id']}")
            elif len(users) == 0:
                body.update({"userName":email, "displayName":displayName})
                creation_response = self.create_user(body)
                if creation_response.status_code == 201:
                    print(f'Created {email}, userid: {creation_response.json()["id"]}')
                else:
                    print(f'Error creating {email}: {creation_response.status_code}')

            elif len(users) > 1:
                print(f'{len(users)} found for {email}, no action taken')

    def delete_user(self, email):
        users = self.find_user_by_email(email).json()['Resources']
        if len(users) == 1:
            user = users[0]
            response = self.delete_user_by_id(user['id'])
            if response.status_code == 204:
                print(f"deleted userid: {user['id']} email: {email}")
                return response
        elif len(users) > 1:
            print('found too many users for email {email}: ')
            for u in users:
                print(u['id'])
        elif len(users) == 0:
            print(f'user {email} not found')

    def delete_user_by_id(self, id:str):
        url = f"{self.base_url}/api/scim/v2/users"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.delete(f"{url}/{id}", headers=headers)
        return response
    
    def document_export(self, id:str)->dict:
        url = f"{self.base_url}/api/unstable/documents/{id}/export"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url,headers=headers)
        return response.json()

    def document_import(self, body:dict):
        url = f"{self.base_url}/api/unstable/documents/import"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.post(url,headers=headers, json=body)
        return response
    
    def list_folders(self, path:str='') -> dict:
        url = f"{self.base_url}/api/unstable/folders"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, 
                                headers=headers, 
                                params={
                                    'path': path,
                                    }
                                )
        return response.json()
    
    def list_documents(self, folderId:str='') -> dict:
        url = f"{self.base_url}/api/unstable/documents"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.get(url, 
                                headers=headers, 
                                params={
                                    'folderId': folderId if folderId else None,
                                    }
                                )
        return response.json() 
    
    @classmethod
    def listify(cls, d:dict) -> dict:
        out = {}
        for k,v in d.items():
            if '[' in v and ']' in v:
                out.update({k:[item for item in v.replace('[','').replace(']','').split(',')]})
            else:
                out.update({k:v})
        return out
    
    def generate_embed_url(self,body:dict) -> dict:
        url = f"{self.base_url}/embed/sso/generate-url"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, json=body)
        return response
