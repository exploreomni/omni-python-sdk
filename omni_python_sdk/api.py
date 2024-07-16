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

    def wait_query_blocking(self, remaining_job_ids:  List[str]) -> Tuple[Any, bool]:
        url = f"{self.base_url}/query/wait"
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        # URL encode the query parameter
        encoded_query = urllib.parse.urlencode({'job_ids': json.dumps(remaining_job_ids)})
        response = requests.get(f"{url}?{encoded_query}", headers=headers)
        
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
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        response = requests.post(url, headers=headers, json=body)
    
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
                return table
            else:
                raise ValueError("No result found in the response.")
        else:
            response.raise_for_status()
