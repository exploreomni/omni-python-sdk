import json
import pytest
from pathlib import Path

# Import example modules
try:
    from examples.topic import Topic
    from examples.databricks_metric_view import metric_view_from_topic
    from examples.snowflake_semantic_view import sematic_view_from_topic
except ImportError:
    pytest.skip("Examples modules not available", allow_module_level=True)


class TestTopicExamples:
    """Test cases for topic functionality from examples."""

    @pytest.fixture
    def topic_data(self):
        """Load topic test data."""
        data_path = Path(__file__).parent / "data" / "order_items.topic.json"
        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @pytest.fixture
    def topic(self, topic_data):
        """Create Topic instance from test data."""
        return Topic.model_validate(topic_data)

    def test_topic_json_loading(self, topic_data):
        """Test that topic JSON data loads as dictionary."""
        assert isinstance(topic_data, dict)
        
    def test_topic_model_validation(self, topic_data):
        """Test Topic model validation from JSON data."""
        topic = Topic.model_validate(topic_data)
        assert isinstance(topic, Topic)
        assert topic.name == "order_items"


class TestDatabricksMetricViewExamples:
    """Test cases for Databricks metric view functionality from examples."""

    @pytest.fixture
    def topic_data(self):
        """Load topic test data."""
        data_path = Path(__file__).parent / "data" / "order_items.topic.json"
        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @pytest.fixture
    def topic(self, topic_data):
        """Create Topic instance from test data."""
        return Topic.model_validate(topic_data)

    @pytest.fixture
    def metric_view(self, topic):
        """Create metric view from topic."""
        return metric_view_from_topic(topic, None, None)

    def test_topic_to_metric_view_conversion(self, topic):
        """Test conversion of Topic to DatabricksMetricView."""
        metric_view = metric_view_from_topic(topic, None, None)
        
        assert metric_view is not None
        assert metric_view.name == "order_items"
        assert metric_view.source == "public.order_items"
        assert len(metric_view.joins) == 0

    def test_metric_view_yaml_generation(self, metric_view):
        """Test YAML generation from metric view."""
        yaml_output = metric_view.generate_yaml()
        
        assert isinstance(yaml_output, str)
        assert len(yaml_output.strip()) > 0
        
        # Load expected YAML for comparison
        expected_path = Path(__file__).parent / "data" / "order_items.databricks_metric_view.yaml"
        with open(expected_path, "r", encoding="utf-8") as f:
            expected_yaml = f.read()
        
        assert yaml_output.strip() == expected_yaml.strip()

    def test_metric_view_sql_generation(self, metric_view):
        """Test SQL generation from metric view."""
        sql_output = metric_view.generate_sql()
        
        assert isinstance(sql_output, str)
        assert len(sql_output.strip()) > 0
        
        # Load expected SQL for comparison
        expected_path = Path(__file__).parent / "data" / "order_items.databricks_metric_view.sql"
        with open(expected_path, "r", encoding="utf-8") as f:
            expected_sql = f.read()
        
        assert sql_output.strip() == expected_sql.strip()


class TestSnowflakeSemanticViewExamples:
    """Test cases for Snowflake semantic view functionality from examples."""

    @pytest.fixture
    def topic_data(self):
        """Load topic test data."""
        data_path = Path(__file__).parent / "data" / "order_items.topic.json"
        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)

    @pytest.fixture
    def topic(self, topic_data):
        """Create Topic instance from test data."""
        return Topic.model_validate(topic_data)

    @pytest.fixture
    def semantic_view(self, topic):
        """Create semantic view from topic."""
        return sematic_view_from_topic(topic)

    def test_topic_to_semantic_view_conversion(self, topic):
        """Test conversion of Topic to SnowflakeSemanticView."""
        semantic_view = sematic_view_from_topic(topic)
        
        assert semantic_view is not None
        assert semantic_view.name == "order_items"
        assert len(semantic_view.tables) == 6

    def test_semantic_view_sql_generation(self, semantic_view):
        """Test SQL generation from semantic view."""
        sql_output = semantic_view.generate_sql()
        
        assert isinstance(sql_output, str)
        assert len(sql_output.strip()) > 0
        
        # Load expected SQL for comparison
        expected_path = Path(__file__).parent / "data" / "order_items.snowflake_semantic_view.sql"
        with open(expected_path, "r", encoding="utf-8") as f:
            expected_sql = f.read()
        
        assert sql_output.strip() == expected_sql.strip()


class TestExamplesIntegration:
    """Integration tests for examples functionality."""

    def test_examples_can_be_imported(self):
        """Test that all example modules can be imported."""
        try:
            from examples import topic, databricks_metric_view, snowflake_semantic_view
            assert topic is not None
            assert databricks_metric_view is not None
            assert snowflake_semantic_view is not None
        except ImportError as e:
            pytest.fail(f"Failed to import examples modules: {e}")

    @pytest.mark.integration
    def test_end_to_end_databricks_workflow(self):
        """Test complete workflow from topic to Databricks metric view."""
        # Load test data
        data_path = Path(__file__).parent / "data" / "order_items.topic.json"
        with open(data_path, "r", encoding="utf-8") as f:
            topic_data = json.load(f)
        
        # Create topic
        topic = Topic.model_validate(topic_data)
        
        # Convert to metric view
        metric_view = metric_view_from_topic(topic, None, None)
        
        # Generate outputs
        yaml_output = metric_view.generate_yaml()
        sql_output = metric_view.generate_sql()
        
        # Verify outputs are generated
        assert isinstance(yaml_output, str)
        assert isinstance(sql_output, str)
        assert len(yaml_output.strip()) > 0
        assert len(sql_output.strip()) > 0

    @pytest.mark.integration
    def test_end_to_end_snowflake_workflow(self):
        """Test complete workflow from topic to Snowflake semantic view."""
        # Load test data
        data_path = Path(__file__).parent / "data" / "order_items.topic.json"
        with open(data_path, "r", encoding="utf-8") as f:
            topic_data = json.load(f)
        
        # Create topic
        topic = Topic.model_validate(topic_data)
        
        # Convert to semantic view
        semantic_view = sematic_view_from_topic(topic)
        
        # Generate output
        sql_output = semantic_view.generate_sql()
        
        # Verify output is generated
        assert isinstance(sql_output, str)
        assert len(sql_output.strip()) > 0