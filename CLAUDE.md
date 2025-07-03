# Claude AI Assistant Instructions

This file contains instructions for AI assistants working on the omni-python-sdk project.

## Project Overview

This is a Python SDK for interacting with the Omni API. The SDK provides a comprehensive interface for:
- Running queries and retrieving data
- User and group management via SCIM API
- Document import/export operations
- Model and topic management
- Embed URL generation

## Development Environment Setup

### Prerequisites
- Python 3.9+
- pip for package management

### Installation
```bash
# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

## Code Quality Tools

### Linting and Formatting
- **Black**: Code formatting (line length: 88)
- **isort**: Import sorting with black profile
- **flake8**: Linting with E203, W503 ignored
- **mypy**: Type checking (strict mode enabled)

### Running Quality Checks
```bash
# Format code
black .

# Sort imports
isort .

# Run linting
flake8

# Type checking
mypy omni_python_sdk/

# Run all pre-commit hooks
pre-commit run --all-files
```

### Testing
- **pytest**: Testing framework
- **pytest-cov**: Coverage reporting

```bash
# Run all tests
make test

# Run tests with coverage
make test-cov

# Run example tests only
make test-examples

# Run integration tests
make test-integration

# Run fast tests (excluding integration)
make test-fast

# Run specific test file
pytest tests/test_api.py -v
```

## Project Structure

```
omni-python-sdk/
├── omni_python_sdk/          # Main package
│   ├── __init__.py
│   └── api.py                 # Core API client
├── tests/                     # Unit tests
│   ├── __init__.py
│   ├── conftest.py           # Test fixtures
│   ├── test_api.py           # API tests
│   ├── test_examples.py      # Example functionality tests
│   ├── data/                 # Test data files
│   └── README.md             # Test documentation
├── examples/                  # Usage examples
├── pyproject.toml            # Project configuration
├── .pre-commit-config.yaml   # Pre-commit hooks
└── CLAUDE.md                 # This file
```

## Key Components

### OmniAPI Class
The main API client class located in `omni_python_sdk/api.py`. Key methods include:
- `run_query_blocking()`: Execute queries and return PyArrow tables
- `create_user()`, `update_user()`, `find_user_by_email()`: User management
- `document_export()`, `document_import()`: Document operations
- `create_topic()`, `update_topic()`, `get_topic()`: Topic management

### Authentication
The SDK supports multiple authentication methods:
- Direct API key and base URL parameters
- Environment variables (OMNI_API_KEY, OMNI_BASE_URL)
- .env file loading

## Development Guidelines

### Code Style
- Follow PEP 8 with Black formatting
- Use type hints for all function parameters and return values
- Add docstrings for all public methods
- Keep line length to 88 characters

### Testing
- Write unit tests for all new functionality
- Use mocks for external API calls
- Maintain high test coverage (aim for >90%)
- Test both success and error scenarios

### Error Handling
- Use the `@requests_error_handler` decorator for API calls
- Provide meaningful error messages
- Handle network timeouts and connection errors gracefully

## Common Tasks

### Adding New API Endpoints
1. Add method to OmniAPI class with proper type hints
2. Use `@requests_error_handler` decorator
3. Follow existing URL building patterns
4. Add comprehensive docstring
5. Write unit tests

### Updating Dependencies
1. Update pyproject.toml
2. Test with new versions
3. Update any breaking changes in code
4. Run full test suite

### Release Process
1. Update version in pyproject.toml
2. Run all quality checks
3. Ensure all tests pass
4. Update changelog/documentation
5. Create release via GitHub

## Troubleshooting

### Common Issues
- Import errors: Check Python path and installation
- API authentication: Verify API key and base URL
- Type checking errors: Ensure all imports have proper type stubs

### Debug Mode
Set environment variable `DEBUG=1` for verbose logging during development.

## Resources

- [Omni API Documentation](https://docs.omni.co)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)