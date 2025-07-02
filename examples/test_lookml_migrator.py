import unittest
from unittest.mock import MagicMock, patch
import lkml

# Mock data and function imports
from lookml_migrator import migrate_lookml

def load_example_lookml(file_path):
    with open(file_path, 'r') as f:
        content = f.read()
    return parse_lkml_content(content)


class TestMigrateLookML(unittest.TestCase):
    def setUp(self):
        # Mock the API client
        self.mock_client = MagicMock()

    def test_migrate_lookml_model(self):
        parsed_model = load_example_lookml("resources/thelook_web_analytics.model.lkml")
        # Run migration
        migrate_lookml(parsed_model, self.mock_client)

        # Check API calls
        self.mock_client.create_model.assert_called_once_with(expected_omni_object)
