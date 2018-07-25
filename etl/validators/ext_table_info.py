from flask_inputs.validators import JsonSchema
from flask_inputs import Inputs
from .constant import cmid

PROPERTIES = {
    "cmid": cmid,
    "table_name": {"type": "string"},
    "ext_pri_key": {"type": "string"},
    "sync_column": {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
    "order_column": {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
    "limit_num": {"type": "integer"},
    "filter": {"type": "string"},
    "filter_format": {"type": "string"},
    "record_num": {"type": "integer"},
    "weight": {"type": "integer"},
    "ext_column": {"type": "object"},
}


class GetExtTableInfos(Inputs):
    _schema = {
        "type": "object",
        "properties": {"cmid": {"type": "string"}},
        "required": ["cmid"],
    }
    args = [JsonSchema(schema=_schema)]


class CreateExtTableInfo(Inputs):
    _schema = {
        "type": "object",
        "required": list(PROPERTIES.keys()),
        "properties": PROPERTIES,
    }

    json = [JsonSchema(schema=_schema)]


class ModifyExtTableInfo(Inputs):
    _schema = {
        "type": "object",
        "properties": PROPERTIES,
    }
    json = [JsonSchema(schema=_schema)]
