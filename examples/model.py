import sys
import json
from omni_python_sdk import OmniAPI

# Example script to update a field in a model with a JSON object
def main(api_key: str, base_url: str, model_id: str, view_name: str, field_name: str, json: dict):
	# Initialize the OmniAPI client
	client = OmniAPI(api_key, base_url)
	
	result = client.update_field(model_id, view_name, field_name, json)
	
	print(result)

if __name__ == "__main__":
	if len(sys.argv) != 7:
		print("Usage: python3 model.py <api_key> <base_url> <model_id> <view_name> <field_name> <json>")
		sys.exit(1)

	api_key = sys.argv[1]
	base_url = sys.argv[2]
	model_id = sys.argv[3]
	view_name = sys.argv[4]
	field_name = sys.argv[5]
	json = json.loads(sys.argv[6])

	main(api_key, base_url, model_id, view_name, field_name, json)