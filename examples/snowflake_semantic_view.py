import sys
from examples.topic import Topic
from omni_python_sdk import OmniAPI
 
# Example of using the OmniAPI to get a topic definition and convert to a Snowflake semantic view
# This example assumes you have a valid API key and base URL for the OmniAPI defined in your .env file

#  https://docs.snowflake.com/en/user-guide/views-semantic/sql#label-semantic-views-create

# CREATE OR REPLACE SEMANTIC VIEW tpch_rev_analysis

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
        sql = f"CREATE OR REPLACE SEMANTIC VIEW OMNI__{self.name}\n\n"

        if self.tables:
            sql += "TABLES (\n"
            for table in self.tables:
                sql += f"  {table['name']} AS {table['table_name']}\n"
                primary_key = table.get("primary_key").split(".")[1]
                if primary_key:
                    sql += f"    PRIMARY KEY ({primary_key})"
                if table.get("synonyms"):
                    sql += f"    WITH SYNONYMS ({', '.join(table['synonyms'])})"
                if table.get("comment"):
                    sql += f"    COMMENT = '{table['comment']}'"
                sql += ",\n"
            sql = sql[:-2] + "\n)\n\n"

        if self.relationships:
            sql += "RELATIONSHIPS (\n"
            for relationship in self.relationships:
                key = relationship.get("source_column").split(".")[1]
                sql += f"  {relationship['name']} AS\n"
                sql += f"    {relationship['source_table']} ({key}) REFERENCES {relationship['target_table']},\n"
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

# remove ${}
def remove_braces(s: str) -> str:
    return s.replace("${", "").replace("}", "")

def transform_sql(s: str) -> str:
    return remove_braces(s)

def sematic_view_from_topic(topic):
    semantic_view = SnowflakeSemanticView(topic.name)
    
    for view in topic.views:
        # skip views without primary keys
        if not view.primary_key[0].field_name:
            continue
        semantic_view.add_table(view.name, table_name=view.fully_scoped_table_name, primary_key=', '.join([k.field_name for k in view.primary_key]), synonyms=view.aliases, comment=view.description)
        for dimension in view.dimensions:
            # skip parameterized dates
            if dimension.date_type:
                continue
            semantic_view.add_dimension(dimension.fully_qualified_field_name, dimension.transform_sql_references(transform_sql), synonyms=dimension.synonyms, comment=dimension.description)
        for measure in view.measures:
            semantic_view.add_metric(measure.fully_qualified_field_name, measure.transform_sql_references(transform_sql), synonyms=measure.synonyms, comment=measure.description)
    
    for relationship in topic.relationships:
        # skip relationships without foreign key to primary key
        if not relationship.on.operands[0].field_name:
            continue
        semantic_view.add_relationship(relationship.id, relationship.left_view_name, relationship.on.operands[0].field_name, relationship.right_view_name)
    
    return semantic_view

def main(model_id: str, topic_name: str):
    client = OmniAPI()

    response = client.get_topic(model_id=model_id, topic_name=topic_name)
    topic = Topic.model_validate(response)
    semantic_view = sematic_view_from_topic(topic)
    print(semantic_view.generate_sql())

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python snowflake_semantic_view.py <model_id> <topic_name>")
        sys.exit(1)

    model_id = sys.argv[1]
    topic_name = sys.argv[2]
    main(model_id, topic_name)
