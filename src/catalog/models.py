from dataclasses import dataclass
@dataclass
class TableMeta:
    schema: str
    table: str
    layer: str
    domain: str
    format: str
@dataclass
class ColumnMeta:
    schema: str
    table: str
    column: str
    type: str
