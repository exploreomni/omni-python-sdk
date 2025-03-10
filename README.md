# omni-python-sdk

Python SDK for interacting with the Omni API

## Installation

```bash
pip install -r requirements.txt
```

## Usage
```python
from omni_python_sdk import OmniAPI

# Set your API key and base URL
api_key = "your_api_key"
base_url = "https://your_domain.omniapp.co"
#these can optionally be set in an .env file with the following keys:
# OMNI_API_KEY=<<your api key>>
# OMNI_BASE_URL=<<your base url>>

# Define your query
query = {
    "query": {
        "sorts": [
            {
                "column_name": "order_items.created_at[date]",
                "sort_descending": False
            }
        ],
        "table": "order_items",
        "fields": [
            "order_items.created_at[date]",
            "order_items.sale_price_sum"
        ],
        "modelId": "your_model_id",
        "join_paths_from_topic_name": "order_items"
    }
}

# Initialize the API with your credentials
api = OmniAPI(api_key, base_url)
# if you've optionally set your keys in a .env file no arguments are required:
# api = OmniAPI()
# if your environment variables are stored in an alternative location
# api = OmniAPI(env_file='<<path_to_custom_env>>')

# Run the query and get a table
table = api.run_query_blocking(query)

# Convert the table to a Pandas DataFrame
df = table.to_pandas()

# Display the first few rows of the DataFrame
print(df.head())
```

To run the example, you need to replace `your_api_key`, `your_domain`, and `your_model_id` with your own values.

To get a query object, you can use the Inspector on a Omni Workbook. The query object is a JSON object that represents the query you want to run. You can find the Inspector in the View menu on a Workbook. Look for the "Query Structure" section.

For a simple command line interface, you can run the following command:

```bash
python3 examples/query.py OMNI_API_KEY https://OMNI_URL '{"query": {"sorts": [{"column_name": "omni_dbt__order_items.created_at[date]", "sort_descending": false}], "table": "omni_dbt__order_items", "fields": ["omni_dbt__order_items.created_at[date]", "omni_dbt__order_items.total_sale_price"], "modelId": "OMNI_MODEL_ID", "join_paths_from_topic_name": "order_items"}}
```
