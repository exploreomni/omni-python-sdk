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
