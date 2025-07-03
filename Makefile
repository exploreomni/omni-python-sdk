.PHONY: help install install-dev test test-cov lint format type-check clean build upload docs pre-commit

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install package
	pip install -e .

install-dev: ## Install package with development dependencies
	pip install -e ".[dev]"

test: ## Run tests
	pytest

test-cov: ## Run tests with coverage
	pytest --cov=omni_python_sdk --cov-report=term-missing --cov-report=html

test-examples: ## Run example tests only
	pytest tests/test_examples.py -v

test-integration: ## Run integration tests
	pytest -m integration -v

test-fast: ## Run tests excluding integration tests
	pytest -m "not integration"

lint: ## Run linting
	flake8 omni_python_sdk/ tests/

format: ## Format code
	black .
	isort .

type-check: ## Run type checking
	mypy omni_python_sdk/

quality: ## Run all quality checks
	black --check .
	isort --check-only .
	flake8 omni_python_sdk/ tests/
	mypy omni_python_sdk/

pre-commit: ## Install pre-commit hooks
	pre-commit install

pre-commit-run: ## Run pre-commit hooks on all files
	pre-commit run --all-files

clean: ## Clean build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean ## Build package
	python -m build

upload-test: build ## Upload to Test PyPI
	twine check dist/*
	twine upload --repository testpypi dist/*

upload: build ## Upload to PyPI
	twine check dist/*
	twine upload dist/*

docs: ## Generate documentation (placeholder)
	@echo "Documentation generation not configured yet"

dev-setup: install-dev pre-commit ## Set up development environment
	@echo "Development environment setup complete!"

.DEFAULT_GOAL := help