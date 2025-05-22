import unittest
import json
from examples.databricks_metric_view import metric_view_from_topic
from examples.topic import Topic

class TestTopicToMetricView(unittest.TestCase):    
    file_path = "examples/tests/data/order_items.topic.json"
    # --- read straight into a dict ---
    with open(file_path, "r", encoding="utf-8") as f:
        json: dict = json.load(f)

    def test_to_metric_view(self):
        # Check if the loaded JSON can be converted to a DatabricksMetricView
        topic = Topic.model_validate(self.json)
        metric_view = metric_view_from_topic(topic)
        self.assertIsNotNone(metric_view)
        self.assertEqual(metric_view.name, "order_items")
        self.assertEqual(metric_view.source, "public.order_items")
        self.assertEqual(len(metric_view.joins), 5)

class TestDatabricksMetricView(unittest.TestCase):
    file_path = "examples/tests/data/order_items.topic.json"
    # --- read straight into a dict ---
    with open(file_path, "r", encoding="utf-8") as f:
        json: dict = json.load(f)
    
    topic = Topic.model_validate(json)
    metric_view = metric_view_from_topic(topic)

    def test_generate_yaml(self):
        # Check if the generated YAML is a string
        yaml = self.metric_view.generate_yaml()
        self.assertIsInstance(yaml, str)
        print(yaml)
        expected_yaml = "examples/tests/data/order_items.databricks_metric_view.yaml"
        with open(expected_yaml, "r", encoding="utf-8") as f:
            expected_yaml_content = f.read()

        self.assertEqual(yaml.strip(), expected_yaml_content.strip())

    def test_generate_sql(self):
        # Check if the generated SQL is a string
        sql = self.metric_view.generate_sql()
        self.assertIsInstance(sql, str)

        expected_sql = "examples/tests/data/order_items.databricks_metric_view.sql"
        with open(expected_sql, "r", encoding="utf-8") as f:
            expected_sql_content = f.read()

        self.assertEqual(sql.strip(), expected_sql_content.strip())
    

if __name__ == "__main__":
    unittest.main()