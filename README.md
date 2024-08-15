# omni-python-sdk

Python SDK for interacting with the Omni API

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python3
from omni_python_sdk import OmniAPI

api_key = "your_api_key"
base_url = "https://your_domain.omniapp.co/api/unstable"
query = {
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

api = OmniAPI(api_key)
table = api.run_query_blocking(query)
df = table.to_pandas()
print(df.head())
```

To run the example, you need to replace `your_api_key`, `your_domain`, and `your_model_id` with your own values.

To get a query object, you can use the Inspector on a Omni Workbook. The query object is a JSON object that represents the query you want to run. You can find the Inspector in the View menu on a Workbook. Look for the "Query Structure" section.

For a simple command line interface, you can run the following command:

```bash
python3 examples/query.py OMNI_API_KEY https://OMNI_URL/api/unstable '{"query": {"sorts": [{"column_name": "omni_dbt__order_items.created_at[date]", "sort_descending": false}], "table": "omni_dbt__order_items", "fields": ["omni_dbt__order_items.created_at[date]", "omni_dbt__order_items.total_sale_price"], "modelId": "OMNI_MODEL_ID", "join_paths_from_topic_name": "order_items"}}
```
