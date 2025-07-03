# Tests

This directory contains the test suite for the omni-python-sdk.

## Test Structure

```
tests/
├── conftest.py          # Shared fixtures and configuration
├── test_api.py          # Core API functionality tests
├── test_examples.py     # Tests for example functionality
├── data/                # Test data files
│   ├── order_items.topic.json
│   ├── order_items.databricks_metric_view.yaml
│   ├── order_items.databricks_metric_view.sql
│   └── order_items.snowflake_semantic_view.sql
└── README.md            # This file
```

## Test Categories

### Unit Tests
- **`test_api.py`**: Tests for the core `OmniAPI` class and its methods
- **`test_examples.py`**: Tests for example functionality (topic, metric views, etc.)

### Integration Tests
- Marked with `@pytest.mark.integration`
- Test complete workflows and interactions between components
- May be slower to run

## Running Tests

### All Tests
```bash
make test
```

### With Coverage
```bash
make test-cov
```

### Example Tests Only
```bash
make test-examples
```

### Integration Tests Only
```bash
make test-integration
```

### Fast Tests (excluding integration)
```bash
make test-fast
```

### Specific Test File
```bash
pytest tests/test_api.py -v
```

### Specific Test Method
```bash
pytest tests/test_api.py::TestOmniAPI::test_init_with_credentials -v
```

## Test Data

Test data files are stored in the `tests/data/` directory and include:

- **`order_items.topic.json`**: Sample topic data for testing conversions
- **`*.yaml`** and **`*.sql`**: Expected output files for validation

## Writing Tests

### Guidelines

1. **Use pytest fixtures** for shared setup and teardown
2. **Mock external dependencies** (API calls, file I/O when appropriate)
3. **Test both success and failure scenarios**
4. **Use descriptive test names** that explain what is being tested
5. **Mark integration tests** with `@pytest.mark.integration`

### Example Test Structure

```python
import pytest
from unittest.mock import Mock, patch
from omni_python_sdk import OmniAPI

class TestOmniAPI:
    def test_method_success_case(self):
        # Test successful execution
        pass
    
    def test_method_error_handling(self):
        # Test error scenarios
        pass
    
    @pytest.mark.integration
    def test_end_to_end_workflow(self):
        # Test complete workflows
        pass
```

### Fixtures

Common fixtures are defined in `conftest.py`:

- `mock_env_vars`: Mocks environment variables for testing
- `sample_query`: Provides sample query data

## Coverage

The test suite aims for high coverage of the core SDK functionality. Coverage reports are generated in HTML format in the `htmlcov/` directory when running `make test-cov`.

## CI/CD Integration

Tests are automatically run in GitHub Actions for:
- Multiple Python versions (3.9, 3.10, 3.11, 3.12)
- Code quality checks (linting, formatting, type checking)
- Coverage reporting

The CI pipeline ensures all tests pass before merging pull requests.