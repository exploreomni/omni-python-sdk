import sys
import json
from uuid import UUID
from omni_python_sdk import AuthenticatedClient
from omni_python_sdk.api.models import models_update_field
from omni_python_sdk.models import ModelsUpdateFieldBody

# Example script to update a field in a model with a JSON object
def main(api_key: str, base_url: str, model_id: str, view_name: str, field_name: str, field_json: dict):
	# Initialize the client
	client = AuthenticatedClient(base_url=base_url, token=api_key)

	result = models_update_field.sync(
		UUID(model_id),
		view_name,
		field_name,
		client=client,
		body=ModelsUpdateFieldBody.from_dict(field_json),
	)

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
	field_json = json.loads(sys.argv[6])

	main(api_key, base_url, model_id, view_name, field_name, field_json)
