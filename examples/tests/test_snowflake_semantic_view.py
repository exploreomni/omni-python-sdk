import unittest
import json
from examples.snowflake_semantic_view import sematic_view_from_topic
from examples.topic import Topic

class TestTopicToSemanticView(unittest.TestCase):    
    file_path = "examples/tests/data/order_items.topic.json"

    # --- read straight into a dict ---
    with open(file_path, "r", encoding="utf-8") as f:
        json: dict = json.load(f)

    def test_to_semantic_view(self):
        # Check if the loaded JSON can be converted to a SnowflakeSemanticView
        topic = Topic.model_validate(self.json)
        semantic_view = sematic_view_from_topic(topic)
        self.assertIsNotNone(semantic_view)
        self.assertEqual(semantic_view.name, "order_items")
        self.assertEqual(len(semantic_view.tables), 6)
 
class TestSnowflakeSemanticView(unittest.TestCase):
    file_path = "examples/tests/data/order_items.topic.json"
    # --- read straight into a dict ---
    with open(file_path, "r", encoding="utf-8") as f:
        json: dict = json.load(f)
    
    topic = Topic.model_validate(json)
    semantic_view = sematic_view_from_topic(topic)

    def test_generate_sql(self):
        # Check if the generated SQL is a string
        sql = self.semantic_view.generate_sql()
        self.assertIsInstance(sql, str)

        expected_sql = "examples/tests/data/order_items.snowflake_semantic_view.sql"
        with open(expected_sql, "r", encoding="utf-8") as f:
            expected_sql_content = f.read()

        self.assertEqual(sql.strip(), expected_sql_content.strip())

if __name__ == "__main__":
    unittest.main()