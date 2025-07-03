# Omni Python SDK

[![CI](https://github.com/exploreomni/omni-python-sdk/actions/workflows/ci.yml/badge.svg)](https://github.com/exploreomni/omni-python-sdk/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/omni-python-sdk.svg)](https://badge.fury.io/py/omni-python-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/omni-python-sdk.svg)](https://pypi.org/project/omni-python-sdk/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive Python SDK for interacting with the Omni API. This library provides a simple and intuitive interface for querying data, managing users and groups, handling documents, and working with Omni's analytics platform.

## Features

- 🔍 **Query Execution**: Run queries and retrieve data as PyArrow tables or Pandas DataFrames
- 👥 **User Management**: Create, update, and manage users via SCIM API
- 📁 **Document Operations**: Import and export Omni documents
- 🏗️ **Model Management**: Create and manage data models and topics  
- 🔗 **Embed URLs**: Generate secure embed URLs for dashboards
- 🛡️ **Type Safety**: Full type hints for better development experience
- ⚡ **Async Support**: Built with performance in mind

## Installation

Install from PyPI:

```bash
pip install omni-python-sdk
```

For development with all optional dependencies:

```bash
pip install omni-python-sdk[dev]
```

## Quick Start

### Basic Usage

```python
from omni_python_sdk import OmniAPI

# Initialize with credentials
api = OmniAPI(
    api_key="your_api_key",
    base_url="https://your_domain.omniapp.co"
)

# Or use environment variables (OMNI_API_KEY, OMNI_BASE_URL)
api = OmniAPI()

# Run a query
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

# Execute query and get results
table, fields = api.run_query_blocking(query)

# Convert to Pandas DataFrame
df = table.to_pandas()
print(df.head())
```

### Environment Configuration

Create a `.env` file in your project root:

```env
OMNI_API_KEY=your_api_key_here
OMNI_BASE_URL=https://your_domain.omniapp.co
```

### User Management

```python
# Find user by email
user = api.return_user_by_email("user@example.com")

# Create or update user
api.upsert_user(
    email="newuser@example.com",
    displayName="New User",
    attributes={"department": "Engineering"}
)

# Add user to group
api.add_user_to_group("Developers", user_id)
```

### Document Operations

```python
# Export document
document_data = api.document_export("document_id")

# Import document
api.document_import(document_data)
```

## Command Line Usage

Run queries directly from the command line:

```bash
python -m examples.query \
  YOUR_API_KEY \
  https://your-domain.omniapp.co \
  '{"query": {"table": "your_table", "fields": ["field1", "field2"], "modelId": "your_model_id"}}'
```

## API Reference

### Core Classes

- **`OmniAPI`**: Main client class for interacting with Omni API
- **`@requests_error_handler`**: Decorator for handling API errors gracefully
- **`@memoized`**: Decorator for caching expensive operations

### Key Methods

| Method | Description |
|--------|-------------|
| `run_query_blocking()` | Execute queries synchronously |
| `create_user()`, `update_user()` | User management operations |
| `document_export()`, `document_import()` | Document operations |
| `create_topic()`, `get_topic()` | Topic management |
| `generate_embed_url()` | Create secure embed URLs |

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/exploreomni/omni-python-sdk.git
cd omni-python-sdk

# Set up development environment
make dev-setup
```

### Code Quality

This project uses several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting  
- **flake8**: Linting
- **mypy**: Type checking
- **pytest**: Testing

```bash
# Run all quality checks
make quality

# Format code
make format

# Run tests with coverage
make test-cov
```

### Testing

```bash
# Run tests
make test

# Run tests with coverage report
make test-cov

# Run specific test file
pytest tests/test_api.py -v
```

## Examples

Explore the [`examples/`](examples/) directory for comprehensive usage examples:

- **Basic Queries**: [`examples/query.py`](examples/query.py)
- **User Management**: [`examples/user_management/`](examples/user_management/)
- **Data Migration**: [`examples/content_migration.py`](examples/content_migration.py)
- **Databricks Integration**: [`examples/databricks_metric_view.py`](examples/databricks_metric_view.py)
- **Snowflake Integration**: [`examples/snowflake_semantic_view.py`](examples/snowflake_semantic_view.py)

## Requirements

- Python 3.9+
- requests
- pyarrow  
- python-dotenv
- pandas (for DataFrame conversion)

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to our GitHub repository.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes and add tests
4. Run quality checks (`make quality`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the MIT License - see the [`LICENSE`](LICENSE) file for details.

## Support

- 📖 **Documentation**: [Omni API Docs](https://docs.omni.co)
- 🐛 **Issues**: [GitHub Issues](https://github.com/exploreomni/omni-python-sdk/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/exploreomni/omni-python-sdk/discussions)

## Changelog

See [`CHANGELOG.md`](CHANGELOG.md) for a detailed history of changes to this project.
