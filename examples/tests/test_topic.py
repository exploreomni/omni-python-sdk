import unittest
import json

from examples.topic import Topic

class TestTopic(unittest.TestCase):
    file_path = "examples/tests/data/order_items.topic.json"

    # --- read straight into a dict --- 
    with open(file_path, "r", encoding="utf-8") as f:
        json: dict = json.load(f)

    def test_json_to_dict(self):
        # Check if the loaded JSON is a dictionary
        self.assertIsInstance(self.json, dict)
        topic = Topic.model_validate(self.json)
        self.assertIsInstance(topic, Topic)
        self.assertEqual(topic.name, "order_items")
        
if __name__ == "__main__":
    unittest.main()