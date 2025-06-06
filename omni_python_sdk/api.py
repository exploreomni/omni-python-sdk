import os
from dotenv import load_dotenv
import requests
import urllib.parse
import pyarrow as pa
import pyarrow.ipc as ipc
import io
import json, ndjson, base64
from typing import List, Tuple, Any, Union
import functools, collections


def requests_error_handler(func):
    """
    Decorator to handle generic errors when raising a status
    via `response.raise_for_status()`. It catches all exceptions that occur when 
    calling the decorated function and prints an error message with exception details.
    Args:
        func (callable): The function to be decorated.
    Returns:
        wrapper (callable): A wrapper function that handles exceptions.
    Raises:
        None (handled internally)
    Example Use:
        @requests_error_handler
        def get_data(url):
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Request Failed: {e}")
            return None
    return wrapper

class memoized(object):
   '''Decorator. Caches a function's return value each time it is called.
   If called later with the same arguments, the cached value is returned
   (not reevaluated).
   '''
   def __init__(self, func):
      self.func = func
      self.cache = {}
   def __call__(self, *args):
      if not isinstance(args, collections.abc.Hashable):
         # uncacheable. a list, for instance.
         # better to not cache than blow up.
         return self.func(*args)
      if args in self.cache:
         return self.cache[args]
      else:
         value = self.func(*args)
         self.cache[args] = value
         return value
   def __repr__(self):
      '''Return the function's docstring.'''
      return self.func.__doc__
   def __get__(self, obj, objtype):
      '''Support instance methods.'''
      return functools.partial(self.__call__, obj)
  
class OmniAPI:
    def __init__(self, api_key: str = '', base_url: str = '',env_file: str = '.env'):
        
        if api_key and base_url:
            self.api_key = api_key
            self.base_url = base_url
        elif load_dotenv(dotenv_path=env_file):
            if os.getenv('OMNI_API_KEY'):
                self.api_key = os.getenv('OMNI_API_KEY')
            else:
                self.api_key = api_key
            if os.getenv('OMNI_BASE_URL'):
                self.base_url = os.getenv('OMNI_BASE_URL')
            else: 
                self.base_url = base_url
        else:
            self.api_key = api_key
            self.base_url = base_url
        self._trim_base_url()
        self.headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

    def _trim_base_url(self) -> None:
        '''
        Trims the base_url to remove any trailing slashes or api versions
        since the versioning of an endpoint is managed by the SDK methods,
        and varies between endpoints
        '''
        if self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]
        if self.base_url.endswith('/api/v1'):
            self.base_url = self.base_url[:-7]
        if self.base_url.endswith('/api'):
            self.base_url = self.base_url[:-4]
        if self.base_url.endswith('/api/unstable'):
            self.base_url = self.base_url[:-13]
            
    @requests_error_handler
    def wait_query_blocking(self, remaining_job_ids: List[str]) -> Tuple[Any, bool]:
        """
        Wait for query jobs to complete.
        Args:
            remaining_job_ids (List[str]): List of job IDs to wait for.
        Returns:
            Tuple[Any, bool]: A tuple containing the response JSON and a boolean indicating if the jobs are done.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        '''
        remaining_job_ids: List[str] - the list of job ids to wait for
        Wait for a query to complete by providing a list of job ids.
        '''
        url = f"{self.base_url}/api/unstable/query/wait"
        
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

    @requests_error_handler
    def run_query_blocking(self, body: dict) -> Tuple[pa.Table, List[dict]]:
        """
        Run a query and wait for its completion.
        Args:
            body (dict): The query body.
        Returns:
            Tuple[pa.Table, List[dict]]: A tuple containing the result table and field information.
        Raises:
            ValueError: If no result is found in the response.
            requests.exceptions.RequestException: If the API request fails.
        """
        url = f"{self.base_url}/api/unstable/query/run"
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

    def _base_model_url(self) -> str:
        """
        Get the base URL for model operations.
        Returns:
            str: The base URL for model operations.
        """
        return f"{self.base_url}/api/unstable/model"

    def _model_url(self, model_id: str) -> str:
        """
        Get the URL for a specific model.
        Args:
            model_id (str): The ID of the model.
        Returns:
            str: The URL for the specified model.
        """
        return f"{self._base_model_url()}/{model_id}"

    def _base_topic_url(self, model_id: str) -> str:
        """
        Get the base URL for topic operations.
        Args:
            model_id (str): The ID of the model.
        Returns:
            str: The base URL for topic operations.
        """
        return f"{self._model_url(model_id)}/topic"

    def _topic_url(self, model_id: str, topic_name: str) -> str:
        """
        Get the URL for a specific topic.
        Args:
            model_id (str): The ID of the model.
            topic_name (str): The name of the topic.
        Returns:
            str: The URL for the specified topic.
        """
        return f"{self._base_topic_url(model_id)}/{topic_name}"

    def _base_view_url(self, model_id: str) -> str:
        """
        Get the base URL for view operations.
        Args:
            model_id (str): The ID of the model.
        Returns:
            str: The base URL for view operations.
        """
        return f"{self._base_model_url()}/{model_id}/view"

    def _view_url(self, model_id: str, view_name: str) -> str:
        """
        Get the URL for a specific view.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view.
        Returns:
            str: The URL for the specified view.
        """
        return f"{self._base_model_url(model_id)}/{view_name}"

    def _base_field_url(self, model_id: str) -> str:
        """
        Get the base URL for field operations.
        Args:
            model_id (str): The ID of the model.
        Returns:
            str: The base URL for field operations.
        """
        return f"{self._base_view_url(model_id)}/field"

    def _field_url(self, model_id: str, view_name: str, field_name: str) -> str:
        """
        Get the URL for a specific field.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view.
            field_name (str): The name of the field.
        Returns:
            str: The URL for the specified field.
        """
        return f"{self._view_url(model_id, view_name)}/field/{field_name}"
    
    @requests_error_handler
    def create_model(self, connection_id: str, body: dict) -> dict:
        """
        Create a new model.
        Args:
            connection_id (str): The connection ID.
            body (dict): The model creation body.
        Returns:
            dict: The created model information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._base_model_url()
        body["connectionId"] = connection_id
        response = requests.post(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def create_topic(self, model_id: str, base_view_name: str, body: dict) -> dict:
        """
        Create a new topic.
        Args:
            model_id (str): The ID of the model.
            base_view_name (str): The name of the base view.
            body (dict): The topic creation body.
        Returns:
            dict: The created topic information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._base_topic_url(model_id)
        body["baseViewName"] = base_view_name
        response = requests.post(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def update_topic(self, model_id: str, topic_name: str, body: dict) -> dict:
        """
        Update an existing topic.
        Args:
            model_id (str): The ID of the model.
            topic_name (str): The name of the topic to update.
            body (dict): The topic update body.
        Returns:
            dict: The updated topic information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._topic_url(model_id, topic_name)
        response = requests.patch(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def delete_topic(self, model_id: str, topic_name: str) -> dict:
        """
        Delete a topic.
        Args:
            model_id (str): The ID of the model.
            topic_name (str): The name of the topic to delete.
        Returns:
            dict: The response from the delete operation.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._topic_url(model_id, topic_name)
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    @requests_error_handler
    def get_topic(self, model_id: str, topic_name: str) -> dict:
        """
        Get a topic by its name.
        Args:
            model_id (str): The ID of the model.
            topic_name (str): The name of the topic to get.
        Returns:
            dict: The topic information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._topic_url(model_id, topic_name)
        response = requests.get(url, headers=self.headers)

        payload = response.json()
        if not payload['success']:
            response.raise_for_status()
        return payload['topic']

    @requests_error_handler
    def create_view(self, model_id: str, view_name: str, body: dict) -> dict:
        """
        Create a new view.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view to create.
            body (dict): The view creation body.
        Returns:
            dict: The created view information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._base_view_url(model_id)
        body["viewName"] = view_name
        response = requests.post(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def update_view(self, model_id: str, view_name: str, body: dict) -> dict:
        """
        Update an existing view.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view to update.
            body (dict): The view update body.
        Returns:
            dict: The updated view information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._view_url(model_id, view_name)
        body["viewName"] = view_name
        response = requests.patch(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def delete_view(self, model_id: str, view_name: str) -> dict:
        """
        Delete a view.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view to delete.
        Returns:
            dict: The response from the delete operation.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._view_url(model_id, view_name)
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def create_field(self, model_id: str, view_name: str, field_name: str, body: dict) -> dict:
        """
        Create a new field.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view.
            field_name (str): The name of the field to create.
            body (dict): The field creation body.
        Returns:
            dict: The created field information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._base_field_url(model_id)
        body["fieldName"] = field_name
        body["viewName"] = view_name
        response = requests.post(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def update_field(self, model_id: str, view_name: str, field_name: str, body: dict) -> dict:
        """
        Update an existing field.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view.
            field_name (str): The name of the field to update.
            body (dict): The field update body.
        Returns:
            dict: The updated field information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._field_url(model_id, view_name, field_name)
        response = requests.patch(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response.json()
    
    @requests_error_handler
    def delete_field(self, model_id: str, view_name: str, field_name: str) -> dict:
        """
        Delete a field.
        Args:
            model_id (str): The ID of the model.
            view_name (str): The name of the view.
            field_name (str): The name of the field to delete.
        Returns:
            dict: The response from the delete operation.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = self._field_url(model_id, view_name, field_name)
        response = requests.delete(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def create_user(self, body: dict) -> requests.Response:
        """
        Create a new user.
        Args:
            body (dict): The user creation body.
        Returns:
            requests.Response: The response from the create operation.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = f"{self.base_url}/api/scim/v2/users"
        response = requests.post(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response

    @requests_error_handler
    def update_user(self, id: str, body: dict) -> requests.Response:
        """
        Update an existing user.
        Args:
            id (str): The ID of the user to update.
            body (dict): The user update body.
        Returns:
            requests.Response: The response from the update operation.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = f"{self.base_url}/api/scim/v2/users/{id}"
        response = requests.put(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response

    @requests_error_handler
    def find_user_by_email(self, email: str) -> requests.Response:
        """
        Find a user by email.
        Args:
            email (str): The email of the user to find.
        Returns:
            requests.Response: The response containing the user information.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        url = f"{self.base_url}/api/scim/v2/users"
        response = requests.get(url, headers=self.headers, params={'filter': f'userName eq "{email}"'})
        response.raise_for_status()
        return response
    
    def return_user_by_email(self, email: str) -> dict:
        """
        Find a user by email and return object
        Args:
            email (str): The email of the user to find.
        Returns:
            dict: A dictionary containing the user's ID and display name.
        Raises:
            requests.exceptions.RequestException: If the API request fails.
        """
        response = self.find_user_by_email(email)
        if response.status_code == 200:
            users = response.json()['Resources']
            if len(users) == 1:
                return users[0]
            else:
                print(f"Found {len(users)} users for {email}")
                return None
        else:
            print(f"Error finding user by email: {response.status_code}")
            return None
    
    def upsert_user(self, email:str, displayName:str, attributes:dict, groups:List[str]=None):
        """
        Create a new user or update an existing user's information.
        Args:
            email (str): The email address of the user.
            displayName (str): The display name for the user.
            attributes (dict): Additional attributes for the user.
        Returns:
            None
        Prints:
            Status messages about the operation's success or failure.
        """
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
        """
        Delete a user by their email address.
        Args:
            email (str): The email address of the user to delete.
        Returns:
            requests.Response: The response object if the user is successfully deleted.
        Prints:
            Status messages about the operation's success or failure.
        """
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

    @requests_error_handler
    def delete_user_by_id(self, id:str):
        """
        Delete a user by their user ID.
        Args:
            id (str): The ID of the user to delete.
        Returns:
            requests.Response: The response object from the delete operation.
        """
        url = f"{self.base_url}/api/scim/v2/users"
        response = requests.delete(f"{url}/{id}", headers=self.headers)
        response.raise_for_status()
        return response

    @requests_error_handler
    def document_export(self, id:str)->dict:
        """
        Export a document by its ID.
        Args:
            id (str): The ID of the document to export.
        Returns:
            dict: The exported document data as a dictionary.
        """
        url = f"{self.base_url}/api/unstable/documents/{id}/export"
        response = requests.get(url,headers=self.headers)
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def document_import(self, body:dict):
        """
        Import a document.
        Args:
            body (dict): The document data to import.
        Returns:
            requests.Response: The response object from the import operation.
        """
        url = f"{self.base_url}/api/unstable/documents/import"
        response = requests.post(url,headers=self.headers, json=body)
        response.raise_for_status()
        return response

    @requests_error_handler
    def list_folders(self, path:str='') -> dict:
        """
        List folders at the specified path.
        Args:
            path (str, optional): The path to list folders from. Defaults to an empty string.
        Returns:
            dict: A dictionary containing the list of folders.
        """
        url = f"{self.base_url}/api/unstable/folders"
        response = requests.get(url, 
                                headers=self.headers, 
                                params={
                                    'path': path,
                                    }
                                )
        response.raise_for_status()
        return response.json()

    @requests_error_handler
    def list_documents(self, folderId:str='') -> dict:
        """
        List documents in the specified folder.
        Args:
            folderId (str, optional): The ID of the folder to list documents from. Defaults to an empty string.
        Returns:
            dict: A dictionary containing the list of documents.
        """
        url = f"{self.base_url}/api/unstable/documents"
        response = requests.get(url, 
                                headers=self.headers, 
                                params={
                                    'folderId': folderId if folderId else None,
                                    }
                                )
        response.raise_for_status()
        return response.json() 
    
    @requests_error_handler
    def list_groups(self, count:int=100,startIndex:int=1) -> dict:
        """
        List folders at the specified path.
        Args:
            count (int): The number of groups to return. Defaults to 100.
            startIndex (int): An integer index that determines the starting point of the sorted result list. Defaults to 1.
        Returns:
            dict: A dictionary containing the list of folders.
        """
        url = f"{self.base_url}/api/scim/v2/groups"
        response = requests.get(url, 
                                headers=self.headers, 
                                params={
                                    'count': count,
                                    'startIndex': startIndex
                                    }
                                )
        response.raise_for_status()
        return response.json()
    
    @requests_error_handler
    def generate_embed_url(self,body:dict) -> dict:
        """
        Generate an embed URL.
        Args:
            body (dict): The request body containing necessary information for generating the embed URL.
        Returns:
            requests.Response: The response object containing the generated embed URL.
        """
        url = f"{self.base_url}/embed/sso/generate-url"
        response = requests.post(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response
    
    @classmethod
    def listify(cls, d:dict) -> dict:
        """
        Convert string representations of lists in a dictionary to actual lists.
        Args:
            d (dict): The input dictionary.
        Returns:
            dict: A new dictionary with string representations of lists converted to actual lists.
        """
        out = {}
        for k,v in d.items():
            if '[' in v and ']' in v:
                out.update({k:[item for item in v.replace('[','').replace(']','').split(',')]})
            else:
                out.update({k:v})
        return out
    
    @memoized
    def get_all_groups(self) -> List[dict]:
        """
        Get all groups.
        Returns:
            List[dict]: A list of dictionaries containing group information.
        """
        groups = []
        count = 100
        startIndex = 1
        while True:
            response = self.list_groups(count, startIndex)
            groups.extend(response['Resources'])
            if response['totalResults'] <= startIndex:
                break
            startIndex += count
        return groups
    
    @memoized
    def get_group_id(self, group_name:str) -> Union[str,None]:
        """
        Get the ID of a group by its name.
        Args:
            group_name (str): The name of the group to get the ID for.
        Returns:
            Union[str,None]: The ID of the group if found, otherwise None.
        """
        groups = self.get_all_groups()
        group = next((group for group in groups if group['displayName'] == group_name), None)
        return group['id'] if group else None
    
    @requests_error_handler
    def get_group(self, group_id:str) -> dict:
        """
        Get a group by its ID.
        Args:
            group_id (str): The ID of the group to get.
        Returns:
            dict: The group information.
        """
        url = f"{self.base_url}/api/scim/v2/groups/{group_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    @requests_error_handler
    def update_group(self, group_id:str, body:dict) -> requests.Response:
        """
        Update a group.
        Args:
            group_id (str): The ID of the group to update.
            body (dict): The update body.
        Returns:
            requests.Response: The response object from the update operation.
        """
        url = f"{self.base_url}/api/scim/v2/groups/{group_id}"
        response = requests.put(url, headers=self.headers, json=body)
        response.raise_for_status()
        return response
    
    @requests_error_handler
    def add_user_to_group(self, group_name:str, user_id:str) -> requests.Response:
        """
        Add a user to a group.
        Args:
            group_name (str): The name of the group to add the user to.
            user_id (str): The ID of the user to add.
        Returns:
            requests.Response: The response object from the add operation.
        """
        group_id = self.get_group_id(group_name)
        if not group_id:
            raise ValueError(f"Group '{group_name}' not found.")
        group = self.get_group(group_id)
        group['members'].append({
            "display": '',
            "value": user_id
        })
        return self.update_group(group_id, group)
    
    @requests_error_handler
    def remove_user_from_group(self, group_name:str, user_id:str) -> requests.Response:
        """
        Remove a user from a group.
        Args:
            group_name (str): The name of the group to remove the user from.
            user_id (str): The ID of the user to remove.
        Returns:
            requests.Response: The response object from the remove operation.
        """
        group_id = self.get_group_id(group_name)
        if not group_id:
            raise ValueError(f"Group '{group_name}' not found.")
        group = self.get_group(group_id)
        group['members'] = [member for member in group['members'] if member['value'] != user_id]
        return self.update_group(group_id, group)
    
    @requests_error_handler
    def get_model_yaml(self, model_id:str, filename:str=None) -> dict:
        """
        Get the YAML representation of a model.
        Args:
            model_id (str): The ID of the model to get.
            filename (str, optional): The name of specific yaml file to return. Defaults to None.
        Returns:
            dict: The YAML representation of the model.
        """
        url = f"{self.base_url}/api/unstable/models/{model_id}/yaml"
        response = requests.get(url, headers=self.headers, params={'filename': filename})
        response.raise_for_status()
        return response.json()
    