import sys
import json
import pandas as pd
from omni_python_sdk import AuthenticatedClient
from omni_python_sdk.helpers import run_query_blocking
from typing import Any, Dict


def main(api_key: str, base_url: str, json_query: Dict[str, Any]):
	# Initialize the client
	client = AuthenticatedClient(base_url=base_url, token=api_key)

	# Execute the query
	result, fields = run_query_blocking(client, json_query)

	# Create a mapping from raw column names to their labels
	column_mapping = {field_name: metadata.get('label', field_name) for field_name, metadata in fields.items()}

	# Convert the result to a pandas DataFrame
	df = result.to_pandas()

	# Reorder the columns in the DataFrame
	df = df[json_query["query"]["fields"]]

	# Rename the columns in the DataFrame
	df.rename(columns=column_mapping, inplace=True)


	# Output the DataFrame as a CSV to the command line
	print(df.to_csv(index=False), end='')

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: python3 query.py <api_key> <base_url> <json_query>")
		sys.exit(1)

	api_key = sys.argv[1]
	base_url = sys.argv[2]
	json_query = json.loads(sys.argv[3])

	main(api_key, base_url, json_query)
