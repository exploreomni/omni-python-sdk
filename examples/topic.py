from pydantic import BaseModel, Field, ConfigDict
from typing import Callable, List, Optional, Literal
from enum import Enum

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


class OmniField(BaseModel):
    fully_qualified_name: str
    field_name: str
    aggregate_type: Optional[AggregateType] = None
    data_type: DataType
    view_name: Optional[str] = None
    sql: Optional[str] = None
    display_sql: Optional[str] = None
    dialect_sql: Optional[str] = None
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
    def fully_qualified_field_name(self):
        if self.view_name:
            return f"{self.view_name}.{self.field_name}"
        else:
            return self.field_name
    
    @property
    def effective_sql(self):
        if self.dialect_sql:
            return self.dialect_sql
        if self.sql:
            return self.sql
        elif self.display_sql:
            return self.display_sql
        else:
            # if not present make sql the column reference
            if self.is_dimension:
                return self.fully_qualified_field_name
    
    def transform_sql_references(self, transformation_function: Callable[[str], str]) -> str:
        # Example transformation: replace ${field_name} and ${view_name.field_name}
        # with transformation function
        return transformation_function(self.effective_sql)

class View(BaseModel):
    name: str
    dimensions: List[OmniField]
    measures: List[OmniField]
    table_name: Optional[str] = None
    
    # renames the *attribute* to `schema_` so it doesn’t clash with BaseModel.schema(),
    # but keeps the JSON key / constructor arg as plain "schema"
    schema_: Optional[str] = Field(default=None, alias="schema")
    catalog: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    primary_key: Optional[List[FieldExpression]] = None
    aliases: Optional[List[str]] = None

    # let View(**{"schema": "sales"}) work, and dump back as {"schema": ...}
    model_config = ConfigDict(populate_by_name=True)

    @property
    def fully_scoped_table_name(self):
        catalog_str = f"{self.catalog}." if self.catalog else ""
        schema_label_str = f"{self.schema_}." if self.schema_ else ""
        table_name_str = f"{self.table_name}" if self.table_name else self.name
        return f"{catalog_str}{schema_label_str}{table_name_str}"
    
    def find_field(self, field_name: str) -> Optional[OmniField]:
        """
        Find a field by name in the view.
        """
        for field in self.dimensions + self.measures:
            if field.field_name == field_name:
                return field
        return None
 
class Topic(BaseModel):
    name: str
    base_view_name: Optional[str] = None
    description: Optional[str] = None
    label: Optional[str] = None
    relationships: List[Relationship]
    views: List[View]

    # ignore unknown keys rather than error
    model_config = {"extra": "ignore"}

    def find_view(self, name: str) -> Optional[View]:
        """
        Find a view by name in the topic.
        """
        for view in self.views:
            if view.name == name:
                return view
        return None
    
    def find_field(self, name: str) -> Optional[OmniField]:
        """
        Find a field by name in the topic.
        """
        view_name, field_name = name.split(".")
        if view_name:
            view = self.find_view(view_name)
            if view:
                return view.find_field(field_name)
        else:
            for view in self.views:
                field = view.find_field(field_name)
                if field:
                    return field
        return None