from pydantic import BaseModel, Field
from typing import Optional, Any, Literal

class QueryRequest(BaseModel):
    question:    str           = Field(..., min_length=2, max_length=500)
    language:    str           = Field("en", description="en | hi | kn | mr | kok")
    session_id:  Optional[str] = None
    include_sql: bool          = False

class QueryResponse(BaseModel):
    question:          str
    answer:            str
    intent:            str                                                              = "SQL"
    data:              Optional[list[dict[str, Any]]]                                   = None
    sql_query:         Optional[str]                                                    = None
    row_count:         int                                                              = 0
    execution_time_ms: int                                                              = 0
    confidence:        str                                                              = "high"
    chart_type:        Optional[Literal["bar","line","doughnut","grouped_bar","forecast","stacked"]] = None
    edge_type:         Optional[str]                                                    = None
    forecast:          Optional[dict[str, Any]]                                         = None
