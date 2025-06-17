import re
from typing import Optional
import yaml
import sys
from examples.topic import Topic
from omni_python_sdk import OmniAPI


# Example of using the OmniAPI to get a topic definition and convert to a Snowflake semantic view
# This example assumes you have a valid API key and base URL for the OmniAPI defined in your .env file

# SQL Reference
# CREATE VIEW
# CREATE [ OR REPLACE ] VIEW [ IF NOT EXISTS ] metric_view_name
#     [ column_list ]
#     WITH METRICS
#     LANGUAGE YAML
#     [ COMMENT view_comment ]
#     [ TBLPROPERTIES clause [...] ]
# AS yaml_definition

# column_list
#    ( { column_alias [ COMMENT column_comment ] } [, ...] )

# yaml_definition
#    $$
#      yaml_string
#    $$

# Example of creating a metric view in Databricks using the YAML format

# CREATE VIEW `my_catalog`.`my_schema`.`my_metric_view_name` 
# (`Order Date` COMMENT "The order date", 
#  `Order Status Readable` COMMENT "O for Open, P for Processing, F for Fulfilled", 
#  `Total Price per customer`)  
# WITH METRICS
# LANGUAGE YAML 
# AS $$ 
# version: 0.1

# source: select * from samples.tpch.orders

# filter: o_orderdate >= '1995-01-01'

# dimensions:
#   - name: Order Date
#     expr: o_orderdate

#   - name: Order Status Readable
#     expr: >
#       case 
#         when o_orderstatus = 'O' then 'Open' 
#         when o_orderstatus = 'P' then 'Processing' 
#         when o_orderstatus = 'F' then 'Fulfilled' 
#       end

# measures:
#   - name: Total Price per customer
#     expr: SUM(o_totalprice) / COUNT(DISTINCT(o_custkey))
# $$

class DatabricksMetricView:
    """
    Class to create a metric view in Databricks using the YAML format.
    """

    def __init__(self, name, default_catalog: Optional[str], default_schema: Optional[str]):
        self.name = name
        self.default_catalog = default_catalog
        self.default_schema = default_schema
        self.version = "0.1"
        # Can be either a fully qualified table-like asset or any SQL query.
        self.source = None
        # Used to LEFT JOIN the fact table defined under source with dimension tables as a star schema model. (Optional)
        self.joins = []
        # A SQL boolean expression; equivalent to the WHERE clause. (Optional)
        self.filter = None
        # An array of columns used in SELECT, WHERE, and GROUP BY expressions.
        self.dimensions = []
        # An array of aggregation expression columns
        self.measures = []
        self.comment = None
    
    def set_source(self, source):
        """
        Set the source for the metric view.
        """
        self.source = source

    def set_filter(self, filter):
        """
        Set the filter for the metric view.
        """
        self.filter = filter

    def set_comment(self, comment):
        """
        Set the comment for the metric view.
        """
        self.comment = comment
    
    def add_join(self, name, source, on):
        """
        Add a join to the metric view.
        """
        join = {
            "name": name,
            "source": source,
            "on": on
        }
        self.joins.append(join)
    
    def add_dimension(self, name, expr, comment=None):
        """
        Add a dimension to the metric view.
        """
        dimension = {
            "name": name,
            "expr": expr,
            "comment": comment
        }
        self.dimensions.append(dimension)
    
    def add_measure(self, name, expr, comment=None):
        """
        Add a measure to the metric view.
        """
        measure = {
            "name": name,
            "expr": expr,
            "comment": comment
        }
        self.measures.append(measure)
    
    def generate_yaml(self):
        """
        Generate the YAML definition for the metric view.
        """
        yaml_definition = {
            "version": self.version,
            "source": self.source,
            # limit dimensions to name and expr
            "dimensions": [
                {"name": dim["name"], "expr": dim["expr"]} for dim in self.dimensions
            ],
            # limit measures to name and expr
            "measures": [
                {"name": measure["name"], "expr": measure["expr"]} for measure in self.measures
            ],         
        }

        if self.filter:
            yaml_definition["filter"] = self.filter
        # if more than one joins are present, add them to the yaml definition
        if len(self.joins) > 0:
            yaml_definition["joins"] = [
            {
                "name": join["name"],
                "source": join["source"],
                "on": join["on"]
            } for join in self.joins
        ]  
        
        return yaml.dump(yaml_definition, default_flow_style=False)

    def column_to_sql(self, column):
        """
        Convert a column definition to SQL.
        """
        optional_comment = f"COMMENT \"{column['comment']}\"" if column.get("comment") else ""
        return f"`{column["name"]}`{optional_comment}"

    def generate_sql(self):
        """
        Generate the SQL statement to create the metric view in Databricks.
        """
        yaml_definition = self.generate_yaml()

        optional_comment = f"COMMENT '{self.comment}'" if self.comment else ""

        # Generate the column list for the SQL statement with dimensions and measures
        column_list = ", ".join(
            [self.column_to_sql(column) for column in self.dimensions + self.measures]
        )
        
        sql = f"""
CREATE OR REPLACE VIEW `OMNI__{self.name}`
({column_list})
WITH METRICS
LANGUAGE YAML
{optional_comment}
AS $$
{yaml_definition}
$$"""
        
        return sql

def transform_name(name: str) -> str:
    """
    Transform the name to be used in the metric view.
    This function can be customized to apply any naming conventions.
    """
    # Example transformation: replace . with underscores and convert to lowercase
    return name.replace(".", "__").lower()

def metric_view_from_topic(topic: Topic, default_catalog: Optional[str], default_schema: Optional[str], enable_joins: bool = False) -> DatabricksMetricView:
    """
    Convert a Topic object to a DatabricksMetricView object.
    """
    metric_view = DatabricksMetricView(name=topic.name, default_catalog=default_catalog, default_schema=default_schema)

    # find the base view
    base_view_name = topic.base_view_name if topic.base_view_name else topic.name
    base_view = topic.find_view(base_view_name)
    if base_view:
        metric_view.set_source(base_view.fully_scoped_table_name(default_catalog, default_schema))
    else:
        raise ValueError(f"Base view {base_view_name} not found in topic {topic.name}")
    metric_view.set_comment(topic.description)

    def transform_sql_references(sql: str) -> str:
        # Example transformation: replace ${field_name} and ${view_name.field_name}
        # with Measure(fully_qualified_field_name) if a measure
        # Pattern to match ${field_name}
        pattern = r"\$\{([^}]+)\}"
        
        def replace_field(match):
            field_name = match.group(1)
            # Check if the field is a measure or dimension
            field = topic.find_field(field_name)
            if field.is_dimension != True:
                return f"Measure({transform_name(field.fully_qualified_field_name)})"
            else:
                # Remove the ${} from the SQL
                return transform_name(field.fully_qualified_field_name)
        return re.sub(pattern, replace_field, sql)

    
    for view in topic.views:
        # skip views that are not the base view if joins are not enabled
        if not enable_joins and view.name != base_view_name:
            continue
        for dimension in view.dimensions:
            # skip parameterized dates
            if dimension.date_type:
                continue
            metric_view.add_dimension(transform_name(dimension.fully_qualified_field_name), dimension.transform_sql_references(transform_sql_references, base_view_name=base_view_name), dimension.description)

        for measure in view.measures:
            metric_view.add_measure(transform_name(measure.fully_qualified_field_name), measure.transform_sql_references(transform_sql_references), measure.description)

    if enable_joins:
        for relationship in topic.relationships:
            # Assuming the relationship is a join
            right_view = topic.find_view(relationship.right_view_name)
            if right_view:
                metric_view.add_join(
                    name=right_view.name,
                    source=right_view.fully_scoped_table_name,
                    on=transform_sql_references(relationship.sql)
                )
            else:
                raise ValueError(f"Right view {relationship.right_view_name} not found in topic {topic.name}")
    return metric_view

def main(model_id: str, topic_name: str, default_catalog: Optional[str], default_schema: Optional[str]):
    client = OmniAPI()

    response = client.get_topic(model_id=model_id, topic_name=topic_name)
    topic = Topic.model_validate(response)
    metric_view = metric_view_from_topic(topic, default_catalog, default_schema)
    print(metric_view.generate_sql())

if __name__ == "__main__":
    num_args = len(sys.argv)
    if not (num_args >= 3 and num_args < 6):
        print("Usage: python databricks_metric_view.py <model_id> <topic_name> <default_catalog> <default_schema>")
        sys.exit(1)

    model_id = sys.argv[1]
    topic_name = sys.argv[2]
    default_catalog = sys.argv[3] if num_args > 3 else None
    default_schema = sys.argv[4] if num_args > 4 else None
    main(model_id, topic_name, default_catalog, default_schema)
