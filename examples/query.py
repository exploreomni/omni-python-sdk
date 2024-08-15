import sys
import json
import pandas as pd
from omni_python_sdk import OmniAPI
from typing import Any, Dict


def main(api_key: str, base_url: str, json_query: Dict[str, Any]):
	# Initialize the OmniAPI client
	client = OmniAPI(api_key, base_url)

	# Execute the query
	result = client.run_query_blocking(json_query)

	# Convert the result to a pandas DataFrame
	df = result.to_pandas()

	# Output the DataFrame as a CSV to the command line
	print(df.to_csv(index=False))

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: python3 query.py <api_key> <base_url> <json_query>")
		sys.exit(1)

	api_key = sys.argv[1]
	base_url = sys.argv[2]
	json_query = json.loads(sys.argv[3])

	main(api_key, base_url, json_query)