from omni_python_sdk import OmniAPI
from pydantic import BaseModel
from typing import List, Optional, Literal
from enum import Enum

# Example of using the OmniAPI to get a topic definition and convert to a Snowflake semantic view
# This example assumes you have a valid API key and base URL for the OmniAPI defined in your .env file

#  https://docs.snowflake.com/en/user-guide/views-semantic/sql#label-semantic-views-create

# CREATE SEMANTIC VIEW tpch_rev_analysis

#   TABLES (
#     orders AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS
#       PRIMARY KEY (o_orderkey)
#       WITH SYNONYMS ('sales orders')
#       COMMENT = 'All orders table for the sales domain',
#     customers AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
#       PRIMARY KEY (c_custkey)
#       COMMENT = 'Main table for customer data',
#     line_items AS SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM
#       PRIMARY KEY (l_orderkey, l_linenumber)
#       COMMENT = 'Line items in orders'
#   )

#   RELATIONSHIPS (
#     orders_to_customers AS
#       orders (o_custkey) REFERENCES customers,
#     line_item_to_orders AS
#       line_items (l_orderkey) REFERENCES orders
#   )

#   FACTS (
#     line_items.line_item_id AS CONCAT(l_orderkey, '-', l_linenumber),
#     orders.count_line_items AS COUNT(line_items.line_item_id),
#     line_items.discounted_price AS l_extendedprice * (1 - l_discount)
#       COMMENT = 'Extended price after discount'
#   )

#   DIMENSIONS (
#     customers.customer_name AS customers.c_name
#       WITH SYNONYMS = ('customer name')
#       COMMENT = 'Name of the customer',
#     orders.order_date AS o_orderdate
#       COMMENT = 'Date when the order was placed',
#     orders.order_year AS YEAR(o_orderdate)
#       COMMENT = 'Year when the order was placed'
#   )

#   METRICS (
#     customers.customer_count AS COUNT(c_custkey)
#       COMMENT = 'Count of number of customers',
#     orders.order_average_value AS AVG(orders.o_totalprice)
#       COMMENT = 'Average order value across all orders',
#     orders.average_line_items_per_order AS AVG(orders.count_line_items)
#       COMMENT = 'Average number of line items per order'
#   )

#   COMMENT = 'Semantic view for revenue analysis';

class SnowflakeSemanticView:
    def __init__(self, name):
        self.name = name
        self.tables = []
        self.relationships = []
        self.facts = []
        self.dimensions = []
        self.metrics = []
        self.comment = ""

    def add_table(self, name, table_name, primary_key, synonyms=None, comment=None):
        table = {
            "name": name,
            "table_name": table_name,
            "primary_key": primary_key,
            "synonyms": synonyms,
            "comment": comment
        }
        self.tables.append(table)

    def add_relationship(self, relationship_name, source_table, source_column, target_table):
        relationship = {
            "name": relationship_name,
            "source_table": source_table,
            "source_column": source_column,
            "target_table": target_table
        }
        self.relationships.append(relationship)

    def add_fact(self, fact_name, expression, comment=None):
        fact = {
            "name": fact_name,
            "expression": expression,
            "comment": comment
        }
        self.facts.append(fact)

    def add_dimension(self, dimension_name, expression, synonyms=None, comment=None):
        dimension = {
            "name": dimension_name,
            "expression": expression,
            "synonyms": synonyms,
            "comment": comment
        }
        self.dimensions.append(dimension)

    def add_metric(self, metric_name, expression, synonyms=None, comment=None):
        metric = {
            "name": metric_name,
            "expression": expression,
            "synonyms": synonyms,
            "comment": comment
        }
        self.metrics.append(metric)

    def set_comment(self, comment):
        self.comment = comment

    def generate_sql(self):
        sql = f"CREATE SEMANTIC VIEW {self.name}\n\n"

        if self.tables:
            sql += "TABLES (\n"
            for table in self.tables:
                sql += f"  {table['name']} AS {table['table_name']}\n"
                if table.get("primary_key"):
                    sql += f"    PRIMARY KEY ({table['primary_key']})"
                if table.get("synonyms"):
                    sql += f"    WITH SYNONYMS ({', '.join(table['synonyms'])})"
                if table.get("comment"):
                    sql += f"    COMMENT = '{table['comment']}'"
                sql += ",\n"
            sql = sql[:-2] + "\n)\n\n"

        if self.relationships:
            sql += "RELATIONSHIPS (\n"
            for relationship in self.relationships:
                sql += f"  {relationship['name']} AS\n"
                sql += f"    {relationship['source_table']} ({relationship['source_column']}) REFERENCES {relationship['target_table']},\n"
            sql = sql[:-2] + "\n)\n\n"
        if self.facts:
            sql += "FACTS (\n"
            for fact in self.facts:
                sql += f"  {fact['name']} AS {fact['expression']}"
                if fact.get("comment"):
                    sql += f" COMMENT = '{fact['comment']}'"
                sql += ",\n"
            sql = sql[:-2] + "\n)\n\n"
        if self.dimensions:
            sql += "DIMENSIONS (\n"
            for dimension in self.dimensions:
                sql += f"  {dimension['name']} AS {dimension['expression']}"
                if dimension.get("synonyms"):
                    sql += f" WITH SYNONYMS ({', '.join(dimension['synonyms'])})"
                if dimension.get("comment"):
                    sql += f" COMMENT = '{dimension['comment']}'"
                sql += ",\n"
            sql = sql[:-2] + "\n)\n\n"
        if self.metrics:
            sql += "METRICS (\n"
            for metric in self.metrics:
                sql += f"  {metric['name']} AS {metric['expression']}"
                if metric.get("comment"):
                    sql += f" COMMENT = '{metric['comment']}'"
                sql += ",\n"
            sql = sql[:-2] + "\n)\n\n"
        if self.comment:
            sql += f"COMMENT = '{self.comment}';\n"
        return sql


class FieldExpression(BaseModel):
    type: Literal["field"]
    field_name: str

class EqualsParsedSqlExpression(BaseModel):
    type: Literal["call"]
    operator: Literal["SqlStdOperatorTable.EQUALS"]
    operands: List[FieldExpression]

class Relationship(BaseModel):
    id: str
    left_view_name: str
    left_view_alias: Optional[str] = None
    right_view_name: str
    right_view_alias: Optional[str] = None
    on: EqualsParsedSqlExpression
    sql: str
    join_type: str
    type: str
    ignored: Optional[bool] = None
    bidirectional: Optional[bool] = None
    extension_model_id: Optional[str] = None

class AggregateType(Enum):
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    SUM = "SUM"
    AVERAGE = "AVERAGE"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"
    PERCENTILE = "PERCENTILE"
    LIST = "LIST"
    AVERAGE_DISTINCT_ON = "AVERAGE_DISTINCT_ON"
    SUM_DISTINCT_ON = "SUM_DISTINCT_ON"
    MEDIAN_DISTINCT_ON = "MEDIAN_DISTINCT_ON"
    PERCENTILE_DISTINCT_ON = "PERCENTILE_DISTINCT_ON"

class DataType(Enum):
    ARRAY = 'ARRAY'
    BOOLEAN = 'BOOLEAN'
    INTERVAL = 'INTERVAL'
    JSON = 'JSON'
    NUMBER = 'NUMBER'
    OTHER_UNGROUPABLE = 'OTHER_UNGROUPABLE'
    STRING = 'STRING'
    TIMESTAMP = 'TIMESTAMP'
    UNKNOWN = 'UNKNOWN'


# remove ${}
def remove_braces(s: str) -> str:
    return s.replace("${", "").replace("}", "")


class OmniField(BaseModel):
    fully_qualified_name: str
    field_name: str
    aggregate_type: Optional[AggregateType] = None
    data_type: DataType
    view_name: Optional[str] = None
    sql: Optional[str] = None
    display_sql: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    is_dimension: Optional[bool] = None
    ai_context: Optional[str] = None
    synonyms: Optional[List[str]] = None
    date_type: Optional[str] = None

    @property
    def aggregate_sql(self):
        if self.aggregate_type:
            return f"{self.aggregate_type.value}({self.clean_sql})"
        else:
            return self.clean_sql
        
    @property
    def clean_sql(self):
        if self.sql:
            return remove_braces(self.sql)
        elif self.display_sql:
            return remove_braces(self.display_sql)
        else:
            # if not present make sql the column reference
            if self.is_dimension:
                return f"{self.view_name}.{self.field_name}"
        
    
class View(BaseModel):
    name: str
    dimensions: List[OmniField]
    measures: List[OmniField]
    table_name: Optional[str] = None
    schema: Optional[str] = None
    catalog: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    primary_key: Optional[List[FieldExpression]] = None
    aliases: Optional[List[str]] = None

    @property
    def fully_scoped_table_name(self):
        catalog_str = f"{self.catalog}." if self.catalog else ""
        schema_label_str = f"{self.schema}." if self.schema else ""
        table_name_str = f"{self.table_name}." if self.table_name else self.name
        return f"{catalog_str}{schema_label_str}{table_name_str}"

class Topic(BaseModel):
    name: str
    relationships: List[Relationship]
    views: List[View]

    # ignore unknown keys rather than error
    model_config = {"extra": "ignore"}

    def to_semantic_view(self):
        semantic_view = SnowflakeSemanticView(self.name)
        
        for view in self.views:
            # skip views without primary keys
            if not view.primary_key[0].field_name:
                continue
            semantic_view.add_table(view.name, table_name=view.fully_scoped_table_name, primary_key=', '.join([k.field_name for k in view.primary_key]), synonyms=view.aliases, comment=view.description)
            for dimension in view.dimensions:
                # skip parameterized dates
                if dimension.date_type:
                    continue
                semantic_view.add_dimension(dimension.field_name, dimension.clean_sql, synonyms=dimension.synonyms, comment=dimension.description)
            for measure in view.measures:
                semantic_view.add_metric(measure.field_name, measure.clean_sql, synonyms=measure.synonyms, comment=measure.description)
        
        for relationship in self.relationships:
            # skip relationships with foreign key to primary key
            if not relationship.on.operands[0].field_name:
                continue
            semantic_view.add_relationship(relationship.id, relationship.left_view_name, relationship.on.operands[0].field_name, relationship.right_view_name)
        
        return semantic_view

def main():
    client = OmniAPI()

    response = client.get_topic("e87ab418-eec4-45d4-877f-cbd88d6c10dc", "order_items")
    
    topic = Topic.model_validate(response)
    semantic_view = topic.to_semantic_view()
    semantic_view.generate_sql()

if __name__ == "__main__":
    main()