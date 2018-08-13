from copy import deepcopy

from flask_inputs import Inputs
from flask_inputs.validators import JsonSchema

PROPERTIES = {
    "source_id": {"type": "string"},
    "table_name": {"type": "string"},
    "alias_table_name": {"type": "string"},
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
        "properties": {
            "source_id": {"type": "string"},
            "table_name": {"type": "string"},
            "weight": {"type": "string", "enum": ["0", "1", "2", ""]},
            "record_num": {"type": "string"},
        },
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
    _optional = deepcopy(PROPERTIES)

    for _v in _optional.values():
        _v["type"] = [_v["type"], "null"]
    _schema = {"type": "object", "properties": _optional}
    json = [JsonSchema(schema=_schema)]
