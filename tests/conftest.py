import pytest
import os
from unittest.mock import patch

@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing."""
    with patch.dict(os.environ, {
        'OMNI_API_KEY': 'test_api_key',
        'OMNI_BASE_URL': 'https://test.omniapp.co'
    }):
        yield

@pytest.fixture
def sample_query():
    """Sample query for testing."""
    return {
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
            "modelId": "test_model_id",
            "join_paths_from_topic_name": "order_items"
        }
    }