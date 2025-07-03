import pytest
from unittest.mock import Mock, patch, MagicMock
import requests
import json
from omni_python_sdk.api import OmniAPI, requests_error_handler

class TestOmniAPI:
    def test_init_with_credentials(self):
        api = OmniAPI(api_key="test_key", base_url="https://test.omniapp.co")
        assert api.api_key == "test_key"
        assert api.base_url == "https://test.omniapp.co"
        assert api.headers["Authorization"] == "Bearer test_key"

    def test_init_with_env_vars(self, mock_env_vars):
        api = OmniAPI()
        assert api.api_key == "test_api_key"
        assert api.base_url == "https://test.omniapp.co"

    def test_trim_base_url(self):
        api = OmniAPI(api_key="test", base_url="https://test.omniapp.co/api/v1/")
        assert api.base_url == "https://test.omniapp.co"

    @patch('requests.post')
    def test_run_query_blocking_success(self, mock_post, sample_query):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '{"result": "dGVzdA=="}\n{"timed_out": "true", "summary": {"fields": []}}'
        mock_post.return_value = mock_response

        api = OmniAPI(api_key="test", base_url="https://test.omniapp.co")
        
        with patch('base64.b64decode') as mock_b64:
            with patch('io.BytesIO') as mock_io:
                with patch('pyarrow.ipc.open_stream') as mock_arrow:
                    mock_table = Mock()
                    mock_reader = Mock()
                    mock_reader.read_all.return_value = mock_table
                    mock_arrow.return_value = mock_reader
                    
                    result = api.run_query_blocking(sample_query)
                    assert result is not None

    @patch('requests.get')
    def test_find_user_by_email_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Resources": [
                {"id": "123", "userName": "test@example.com", "displayName": "Test User"}
            ]
        }
        mock_get.return_value = mock_response

        api = OmniAPI(api_key="test", base_url="https://test.omniapp.co")
        response = api.find_user_by_email("test@example.com")
        
        assert response.status_code == 200
        mock_get.assert_called_once()

    def test_requests_error_handler_decorator(self):
        @requests_error_handler
        def failing_function():
            raise Exception("Test error")
        
        result = failing_function()
        assert result is None

class TestMemoized:
    def test_memoization(self):
        from omni_python_sdk.api import memoized
        
        call_count = 0
        
        @memoized
        def test_func(x):
            nonlocal call_count
            call_count += 1
            return x * 2
        
        # First call
        result1 = test_func(5)
        assert result1 == 10
        assert call_count == 1
        
        # Second call with same args should use cache
        result2 = test_func(5)
        assert result2 == 10
        assert call_count == 1  # Should not increment